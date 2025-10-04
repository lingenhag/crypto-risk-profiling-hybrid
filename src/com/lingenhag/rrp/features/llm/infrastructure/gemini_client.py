# src/com/lingenhag/rrp/features/llm/infrastructure/gemini_client.py
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests  # type: ignore[import-untyped]

from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.llm.application.ports import LlmPort


_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)


def _strip_json_fences(text: str) -> str:
    m = _JSON_FENCE_RE.match(text.strip())
    return m.group(1) if m else text


@dataclass(frozen=True)
class GeminiClient(LlmPort):
    api_key: Optional[str] = None
    model: str = "gemini-1.5-pro"
    endpoint: str = "https://generativelanguage.googleapis.com/v1beta/models"
    timeout: int = 60
    prompt_file: str = "prompts/summarize_sentiment.txt"
    metrics: Optional[Metrics] = None
    max_tokens: int = 400
    temperature: float = 0.0
    response_mime_type: str = "application/json"

    # Neu: konfigurierbare Caps/Autoscaling
    max_output_tokens_cap: Optional[int] = None
    auto_scale_max_tokens: bool = True

    def summarize_and_score(
            self,
            asset_symbol: str,
            url: str,
            published_at: Optional[str] = None,
            title: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[Any], Optional[Any]]:
        key = self.api_key or os.getenv("GEMINI_API_KEY")
        if not key:
            raise RuntimeError("GEMINI_API_KEY fehlt.")

        if not os.path.exists(self.prompt_file):
            raise FileNotFoundError(f"Prompt-Datei fehlt: {self.prompt_file}")

        with open(self.prompt_file, "r", encoding="utf-8") as f:
            template = f.read()

        user_prompt = (
            template.replace("{{asset_symbol}}", asset_symbol)
            .replace("{{url}}", url)
            .replace("{{published_at}}", str(published_at or "null"))
            .replace("{{title}}", title or "")
        )

        url_api = f"{self.endpoint}/{self.model}:generateContent?key={key}"

        def _mk_payload(max_output_tokens: int) -> Dict[str, Any]:
            return {
                "contents": [{"parts": [{"text": user_prompt}]}],
                "generationConfig": {
                    "temperature": float(self.temperature),
                    "maxOutputTokens": int(max_output_tokens),
                    "response_mime_type": self.response_mime_type,
                },
            }

        def _call_once(max_output_tokens: int) -> Dict[str, Any]:
            start_time = time.time()
            try:
                resp = requests.post(url_api, json=_mk_payload(max_output_tokens), timeout=self.timeout)
                if resp.status_code >= 400:
                    raise RuntimeError(f"Gemini API error {resp.status_code}: {resp.text}")
                data = resp.json()
                if self.metrics:
                    self.metrics.track_api_request("gemini", "success")
                    self.metrics.track_api_duration("gemini", time.time() - start_time)
                return data
            except Exception:
                if self.metrics:
                    self.metrics.track_api_request("gemini", "error")
                    self.metrics.track_api_duration("gemini", time.time() - start_time)
                raise

        # Autoscaling-Loop
        max_out = max(64, int(self.max_tokens))
        cap = int(self.max_output_tokens_cap) if self.max_output_tokens_cap else 2048
        attempts = 0
        while True:
            attempts += 1
            data = _call_once(max_out)

            try:
                cand = (data.get("candidates") or [])[0]
            except Exception:
                raise RuntimeError(f"Gemini API returned unexpected response: {data}")

            finish = cand.get("finishReason") or cand.get("finish_reason")
            parts = ((cand.get("content") or {}).get("parts") or [])
            text = parts[0].get("text") if parts and isinstance(parts[0], dict) else None

            # MAX_TOKENS → eskalieren, falls erlaubt
            if finish == "MAX_TOKENS" and self.auto_scale_max_tokens and max_out < cap:
                new_max = min(cap, max_out + 400)
                if new_max > max_out:
                    max_out = new_max
                    continue  # retry mit größerem Budget

            if not text or not text.strip():
                raise RuntimeError(f"Gemini API returned unexpected response: {data}")

            # JSON robust parsen (Codefences entfernen)
            raw = _strip_json_fences(text)
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # fallback: einfache Heuristik – entferne Typo-Zeilen/Trailing-Komma
                cleaned = raw.strip().rstrip(",")
                parsed = json.loads(cleaned)

            # Normalisierung
            if "sentiment" in parsed:
                try:
                    s = float(parsed["sentiment"])
                except Exception:
                    s = 0.0
                parsed["sentiment"] = max(-1.0, min(1.0, s))

            if "relevance" in parsed:
                parsed["relevance"] = bool(parsed["relevance"])

            return parsed, None, None