# src/com/lingenhag/rrp/features/llm/application/ports.py
from __future__ import annotations
from typing import Protocol, Optional, Tuple, Dict, Any

from com.lingenhag.rrp.domain.models import SummarizedArticle, SentimentDecision, RelevanceDecision


class LlmPort(Protocol):
    model: str

    def summarize_and_score(
            self,
            asset_symbol: str,
            url: str,
            published_at: Optional[str] = None,
            title: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Optional[SentimentDecision], Optional[RelevanceDecision]]:
        ...


class VotesRepositoryPort(Protocol):
    def save_vote(
            self,
            url: str,
            asset_symbol: str,
            model: str,
            relevance: bool,
            sentiment: Optional[float],
            summary: Optional[str],
            harvest_id: Optional[int] = None,
            article_id: Optional[int] = None,
    ) -> int:
        ...

    def save_summary(self, article: SummarizedArticle) -> int:
        ...

    def save_rejection(
            self,
            url: str,
            asset_symbol: str,
            reason: str,
            source: Optional[str],
            context: str,
            article_id: Optional[int] = None,
            *,
            model: Optional[str] = None,
            details_json: Optional[str] = None,
    ) -> int:
        ...