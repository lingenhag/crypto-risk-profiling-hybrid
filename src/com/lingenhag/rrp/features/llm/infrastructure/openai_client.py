# src/com/lingenhag/rrp/features/llm/infrastructure/openai_client.py
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import openai
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.llm.application.ports import LlmPort


_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)


def _strip_json_fences(text: str) -> str:
    m = _JSON_FENCE_RE.match(text.strip())
    return m.group(1) if m else text


@dataclass(frozen=True)
class OpenAIClient(LlmPort):
    """
    OpenAI-Adapter mit robuster JSON-Parsing-Logik und sauberem Return-Typ.
    Wichtig: summarize_and_score MUSS (Dict, None, None) liefern, damit das Ensemble
    nicht mit "too many values to unpack" crasht.
    """
    api_key: Optional[str] = None
    model: str = "gpt-4o-mini"  # Bitte in config.yaml setzen; 'gpt-5' ist i. d. R. ungültig.
    endpoint: str = "https://api.openai.com/v1"  # Base-URL (ohne /chat/completions)
    timeout: int = 60
    fallback_model: Optional[str] = None
    prompt_file: str = "prompts/summarize_sentiment.txt"
    metrics: Optional[Metrics] = None
    max_tokens: int = 400
    temperature: float = 0.0
    response_format: str = "json_object"

    # Optionale Autoscaling-Parameter (paritätisch zu anderen Clients)
    max_tokens_cap: Optional[int] = None
    auto_scale_max_tokens: bool = True

    # interner Client (mutable; via object.__setattr__)
    client: openai.OpenAI = field(init=False, repr=False)

    def __post_init__(self) -> None:  # noqa: D401
        """
        Initialisiert den OpenAI-Client. Da die Dataclass frozen=True ist,
        benutzen wir object.__setattr__ für das mutable Feld.
        """
        # eventuelle Fehlkonfiguration wie ".../chat/completions" am Ende strippen
        base_url = re.sub(r"/chat/completions/?$", "", (self.endpoint or "").rstrip("/")) or "https://api.openai.com/v1"

        client = openai.OpenAI(
            api_key=self.api_key or os.getenv("OPENAI_API_KEY"),
            base_url=base_url,
            timeout=self.timeout,
        )
        object.__setattr__(self, "client", client)

    def _build_user_prompt(self, *, asset_symbol: str, url: str, published_at: Optional[str], title: Optional[str]) -> str:
        if not os.path.exists(self.prompt_file):
            raise FileNotFoundError(f"Prompt-Datei fehlt: {self.prompt_file}")
        with open(self.prompt_file, "r", encoding="utf-8") as f:
            template = f.read()
        return (
            template.replace("{{asset_symbol}}", asset_symbol)
            .replace("{{url}}", url)
            .replace("{{published_at}}", str(published_at or "null"))
            .replace("{{title}}", title or "")
        )

    def _parse_json_content(self, content: str) -> Dict[str, Any]:
        """
        Entfernt ggf. Code-Fences und parst robust zu Dict.
        """
        raw = _strip_json_fences(content or "")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            cleaned = raw.strip().rstrip(",")
            parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise RuntimeError("OpenAI JSON-Antwort ist kein Objekt.")
        return parsed

    @staticmethod
    def _normalize(parsed: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalisiert Keys/Werte für Ensemble:
        - relevance → bool
        - sentiment → -1..1 (float)
        - summary → str
        """
        out: Dict[str, Any] = dict(parsed)

        if "relevance" in out:
            out["relevance"] = bool(out["relevance"])

        if "sentiment" in out:
            try:
                s = float(out["sentiment"])
            except Exception:
                s = 0.0
            out["sentiment"] = max(-1.0, min(1.0, s))

        out["summary"] = (out.get("summary") or "").strip()
        return out

    def _call_once(self, *, model_name: str, user_prompt: str, max_tokens: int) -> Dict[str, Any]:
        start = time.time()
        try:
            resp = self.client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "Du bist ein präziser Finanz-Analyst."},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=int(max_tokens),
                temperature=float(self.temperature),
                response_format={"type": self.response_format},  # "json_object"
            )
            data = resp.model_dump()
            if self.metrics:
                self.metrics.track_api_request("openai", "success")
                self.metrics.track_api_duration("openai", time.time() - start)
            return data
        except Exception as e:
            if self.metrics:
                self.metrics.track_api_request("openai", "error")
                self.metrics.track_api_duration("openai", time.time() - start)
            raise

    def _extract_content(self, data: Dict[str, Any]) -> str:
        """
        Holt choices[0].message.content als String.
        Einige Clients liefern bereits ein Objekt; wir normalisieren auf String → JSON.
        """
        try:
            choices = data.get("choices") or []
            if not choices:
                raise KeyError("choices leer")
            msg = (choices[0].get("message") or {})
            content = msg.get("content")
            if content is None:
                raise KeyError("message.content fehlt")
            # Falls die Bibliothek bereits ein dict liefert (selten), serialisieren
            if isinstance(content, dict):
                return json.dumps(content, ensure_ascii=False)
            return str(content)
        except Exception as e:
            raise RuntimeError(f"OpenAI response format unerwartet: {e} | payload={json.dumps(data)[:500]}")

    def summarize_and_score(
            self,
            asset_symbol: str,
            url: str,
            published_at: Optional[str] = None,
            title: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[Any], Optional[Any]]:
        """
        Erwarteter Rückgabewert (für Ensemble):
            (parsed_dict, None, None)
        wobei parsed_dict die Keys {"relevance": bool, "summary": str, "sentiment": float} enthält.
        """
        user_prompt = self._build_user_prompt(
            asset_symbol=asset_symbol, url=url, published_at=published_at, title=title
        )

        # Einfacher Call (Fallback bei Fehlern)
        def _run_with_model(model_name: str) -> Tuple[Dict[str, Any], Optional[Any], Optional[Any]]:
            max_out = max(64, int(self.max_tokens))
            cap = int(self.max_tokens_cap) if self.max_tokens_cap is not None else 4096

            while True:
                data = self._call_once(model_name=model_name, user_prompt=user_prompt, max_tokens=max_out)
                content = self._extract_content(data)

                # Manche Fehler resultieren in abgeschnittenen Objekten → bei JSON-Fehler ggf. Tokenlimit erhöhen
                try:
                    parsed = self._parse_json_content(content)
                except json.JSONDecodeError:
                    if self.auto_scale_max_tokens and max_out < cap:
                        max_out = min(cap, max_out + 400)
                        continue
                    raise RuntimeError(f"OpenAI JSON konnte nicht geparst werden (model={model_name}).")

                norm = self._normalize(parsed)
                # Mindestfelder absichern (wie bei Gemini/XAI)
                if "relevance" not in norm:
                    # Heuristik: Wenn summary leer, relevance False
                    norm["relevance"] = bool(norm.get("summary"))
                if "sentiment" not in norm:
                    norm["sentiment"] = 0.0
                if "summary" not in norm:
                    norm["summary"] = ""

                return norm, None, None

        try:
            return _run_with_model(self.model)
        except Exception:
            if self.fallback_model:
                return _run_with_model(self.fallback_model)
            raise