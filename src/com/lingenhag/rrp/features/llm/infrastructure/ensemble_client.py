# src/com/lingenhag/rrp/features/llm/infrastructure/ensemble_client.py
from __future__ import annotations
import logging
from statistics import mean
from typing import Any, Dict, List, Optional, Protocol, Tuple

from com.lingenhag.rrp.features.llm.application.ports import LlmPort

_LOG = logging.getLogger(__name__)


class _LLMProtocol(Protocol):
    model: str

    def summarize_and_score(
            self,
            asset_symbol: str,
            url: str,
            published_at: Optional[str] = None,
            title: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[Any], Optional[Any]]:
        ...


class EnsembleClient(LlmPort):
    def __init__(self, clients: List[_LLMProtocol]) -> None:
        self.clients = [c for c in clients if c is not None]
        names = ", ".join(f"{c.__class__.__name__}({getattr(c, 'model', '?')})" for c in self.clients)
        _LOG.info("[ensemble] aktive LLMs: %s", names if names else "keine aktiven LLMs")

    @property
    def model(self) -> str:
        """
        Kennzeichnet das Ensemble – NUR die Modellauswahl, keine Scores.
        Beispiel: ensemble[gpt-5,gemini-2.5-flash,grok-4]
        """
        inner = ",".join(getattr(c, "model", "?") for c in self.clients)
        return f"ensemble[{inner}]" if inner else "ensemble[]"

    def summarize_and_score(
            self,
            asset_symbol: str,
            url: str,
            published_at: Optional[str] = None,
            title: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[Any], Optional[Any]]:
        votes: List[Dict[str, Any]] = []
        for client in self.clients:
            try:
                raw, _, _ = client.summarize_and_score(
                    asset_symbol=asset_symbol,
                    url=url,
                    published_at=published_at,
                    title=title,
                )
                vote = self._normalize_vote(raw, model=getattr(client, "model", client.__class__.__name__))
                votes.append(vote)
            except Exception as e:
                _LOG.warning("LLM call failed (%s): %s", getattr(client, "model", client.__class__.__name__), e)

        agg_relevance, relevant_votes = self._aggregate_relevance(votes)
        agg_sentiment = self._aggregate_sentiment(votes)  # KEINE Rundung hier – Tests erwarten exakten Mittelwert
        agg_summary = self._pick_summary(relevant_votes, fallback_all=votes)

        return {
            "relevance": agg_relevance,
            "sentiment": agg_sentiment,
            "summary": agg_summary or "",
            "votes": votes,  # enthält pro Modell den einzelnen (gerundeten) Vote
        }, None, None

    @staticmethod
    def _normalize_vote(raw: Dict[str, Any], model: str) -> Dict[str, Any]:
        rel = bool(raw.get("relevance"))
        sentiment = None
        if raw.get("sentiment") is not None:
            try:
                s = float(raw.get("sentiment"))
            except Exception:
                s = 0.0
            s = max(-1.0, min(1.0, s))
            sentiment = round(s, 2)  # 2 Nachkommastellen für Einzelvote
        summary = (raw.get("summary") or "").strip()
        return {"model": model, "relevance": rel, "sentiment": sentiment, "summary": summary}

    @staticmethod
    def _aggregate_relevance(votes: List[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]]]:
        if not votes:
            return False, []
        trues = sum(1 for v in votes if bool(v.get("relevance")))
        falses = len(votes) - trues
        agg = trues >= falses
        relevant = [v for v in votes if bool(v.get("relevance"))]
        return agg, relevant

    @staticmethod
    def _aggregate_sentiment(votes: List[Dict[str, Any]]) -> Optional[float]:
        ss = [float(v["sentiment"]) for v in votes if v.get("sentiment") is not None]
        # WICHTIG: keine Rundung – Tests erwarten den exakten Mittelwert.
        return mean(ss) if ss else None

    @staticmethod
    def _pick_summary(relevant: List[Dict[str, Any]], fallback_all: List[Dict[str, Any]]) -> str:
        for v in relevant:
            s = (v.get("summary") or "").strip()
            if s:
                return s
        for v in fallback_all:
            s = (v.get("summary") or "").strip()
            if s:
                return s
        return ""