# src/com/lingenhag/rrp/features/llm/infrastructure/xai_client.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
import requests  # type: ignore[import-untyped]
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.llm.application.ports import LlmPort


@dataclass(frozen=True)
class XAIClient(LlmPort):
    api_key: Optional[str] = None
    model: str = "grok-4"
    endpoint: str = "https://api.x.ai/v1/chat/completions"
    timeout: int = 60
    prompt_file: str = "prompts/summarize_sentiment.txt"
    max_retries: int = 3
    metrics: Optional[Metrics] = None
    max_tokens: int = 1200
    temperature: float = 0.0
    max_tokens_cap: Optional[int] = None
    auto_scale_max_tokens: bool = True

    def fetch_url_content(self, url: str) -> str:
        start_time = time.time()
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            paragraphs = soup.find_all("p")
            content = " ".join(p.get_text().strip() for p in paragraphs if p.get_text().strip())[:1000]
            result = content if content else "Kein lesbarer Inhalt gefunden."
            if self.metrics:
                self.metrics.track_api_request("xai_content", "success")
                self.metrics.track_api_duration("xai_content", time.time() - start_time)
            return result
        except requests.RequestException as e:
            if self.metrics:
                self.metrics.track_api_request("xai_content", "error")
                self.metrics.track_api_duration("xai_content", time.time() - start_time)
            return f"Fehler beim Abrufen der URL: {str(e)}"

    def _call_api(self, payload: Dict[str, Any], attempt: int = 1) -> Dict[str, Any]:
        key = self.api_key or os.getenv("XAI_API_KEY")
        if not key:
            raise RuntimeError("XAI_API_KEY fehlt.")

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

        start_time = time.time()
        try:
            resp = requests.post(self.endpoint, headers=headers, json=payload, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict) or "choices" not in data or not data["choices"]:
                raise ValueError(f"Ungültiges API-Response-Format: {json.dumps(data, default=str, indent=2)}")

            choice = data["choices"][0]
            if not isinstance(choice, dict) or not choice.get("message") or not choice["message"].get("content"):
                raise ValueError(f"Ungültiges choice-Format: {json.dumps(data, default=str, indent=2)}")

            if self.metrics:
                self.metrics.track_api_request("xai", "success")
                self.metrics.track_api_duration("xai", time.time() - start_time)
            return data
        except (requests.RequestException, ValueError) as e:
            if self.metrics:
                self.metrics.track_api_request("xai", "error")
                self.metrics.track_api_duration("xai", time.time() - start_time)
            if attempt < self.max_retries:
                return self._call_api(payload, attempt + 1)
            raise RuntimeError(f"XAI API Fehler nach {self.max_retries} Versuchen: {str(e)}") from e

    def summarize_and_score(
            self,
            asset_symbol: str,
            url: str,
            published_at: Optional[str] = None,
            title: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[Any], Optional[Any]]:
        if not asset_symbol or not asset_symbol.strip():
            raise ValueError("asset_symbol darf nicht leer sein.")
        if not url or not url.strip():
            raise ValueError("url darf nicht leer sein.")

        if not os.path.exists(self.prompt_file):
            raise FileNotFoundError(f"Prompt-Datei nicht gefunden: {self.prompt_file}")

        with open(self.prompt_file, "r", encoding="utf-8") as f:
            template = f.read().strip()
            if not template:
                raise ValueError(f"Prompt-Datei ist leer: {self.prompt_file}")

        url_content = self.fetch_url_content(url)

        user_prompt = (
            template.replace("{{asset_symbol}}", asset_symbol.strip())
            .replace("{{url}}", url.strip())
            .replace("{{published_at}}", published_at or "")
            .replace("{{title}}", title or "")
            .replace("{{url_content}}", url_content)
        )

        def _payload(max_tokens_curr: int) -> Dict[str, Any]:
            return {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "Du bist ein präziser Finanz-Analyst."},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": self.temperature,
                "max_tokens": max_tokens_curr,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "analysis_response",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "relevance": {"type": "boolean"},
                                "summary": {"type": "string"},
                                "sentiment": {"type": "number", "minimum": -1, "maximum": 1},
                            },
                            "required": ["relevance", "summary", "sentiment"],
                            "additionalProperties": False,
                        },
                    },
                    "strict": True,
                },
            }

        max_out = max(64, int(self.max_tokens))
        cap = int(self.max_tokens_cap) if self.max_tokens_cap is not None else 4096

        for attempt in range(1, self.max_retries + 1):
            data = self._call_api(_payload(max_out), attempt=attempt)
            content = data["choices"][0]["message"]["content"]
            if not content or not content.strip():
                if self.auto_scale_max_tokens and max_out < cap:
                    max_out = min(cap, max_out + 400)
                    continue
                raise RuntimeError(
                    f"Leere Antwort\nPrompt:\n{user_prompt}\nResponse:\n{json.dumps(data, default=str, indent=2)}"
                )
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError:
                if self.auto_scale_max_tokens and max_out < cap:
                    max_out = min(cap, max_out + 400)
                    continue
                raise RuntimeError(
                    f"Parsen-Fehler\nPrompt:\n{user_prompt}\nResponse:\n{json.dumps(data, default=str, indent=2)}"
                )

            if not isinstance(parsed, dict):
                raise RuntimeError("Antwort ist kein Dictionary")
            required_keys = {"relevance", "summary", "sentiment"}
            if not all(key in parsed for key in required_keys):
                raise RuntimeError(f"Fehlende Schlüssel: {required_keys - set(parsed.keys())}")

            if not isinstance(parsed["relevance"], bool):
                raise RuntimeError("relevance muss Boolean sein")
            if not isinstance(parsed["summary"], str):
                raise RuntimeError("summary muss String sein")
            if not isinstance(parsed["sentiment"], (int, float)):
                raise RuntimeError("sentiment muss eine Zahl sein")

            parsed["sentiment"] = max(-1.0, min(1.0, float(parsed["sentiment"])))
            return parsed, None, None

        raise RuntimeError("XAI API failed after retries.")