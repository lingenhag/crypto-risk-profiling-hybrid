# src/com/lingenhag/rrp/domain/interfaces.py
from __future__ import annotations
from typing import Protocol, runtime_checkable, Optional, List, Dict, Any
from datetime import datetime
from ch.bfh.pm.domain.models import (
    MarketSnapshot, SentimentDecision, RelevanceDecision, Article, SearchCriteria,
    POmegaScore, RiskFactor, CryptoAsset
)


@runtime_checkable
class RelevanceClassifier(Protocol):
    """Classifies article relevance for a crypto asset."""

    def classify(
            self, asset_symbol: str, title: str, content: str, meta: Dict[str, Any]
    ) -> RelevanceDecision:
        ...


@runtime_checkable
class SentimentAnalyzer(Protocol):
    """Analyzes sentiment of text content."""

    def analyze(self, text: str) -> SentimentDecision:
        ...


@runtime_checkable
class ContentFetcher(Protocol):
    """Fetches full content from a URL."""

    def fetch(self, url: str) -> Optional[str]:
        ...


@runtime_checkable
class UrlResolver(Protocol):
    """Resolves and cleans URLs for harvesting."""

    def resolve(self, url: str) -> Optional[str]:
        ...


@runtime_checkable
class MarketDataSource(Protocol):
    """Fetches market data snapshots from external providers."""

    def fetch_spot(
            self, *, provider_ids: List[str], vs_currency: str = "usd"
    ) -> List[MarketSnapshot]:
        """
        Fetches current spot market data for given provider IDs.
        """

    def fetch_history_range(
            self,
            *,
            provider_id: str,
            vs_currency: str,
            ts_from: int,
            ts_to: int,
            granularity: str = "hourly",
    ) -> List[MarketSnapshot]:
        """
        Fetches historical market data as snapshots in the given range.
        """


@runtime_checkable
class NewsAggregatorPort(Protocol):
    """Aggregates news articles based on search criteria."""

    def fetch_articles(self, criteria: SearchCriteria) -> List[Article]:
        """
        Fetches relevant articles for an asset in the time range.
        Supports languages and limits.
        """


@runtime_checkable
class LLMAdapterPort(Protocol):
    """Adapts LLM for relevance and sentiment analysis (ensemble support)."""

    def process_articles(
            self, articles: List[Article], dry_run: bool = False
    ) -> List[RelevanceDecision]:
        """
        Processes articles for relevance; extends to sentiment if relevant.
        Dry-run skips API calls.
        """

    def analyze_sentiment_batch(
            self, texts: List[str], model: str = "gpt-4o"
    ) -> List[SentimentDecision]:
        """
        Batch-analyzes sentiment with rate-limit handling.
        """


@runtime_checkable
class RiskCalculatorPort(Protocol):
    """Calculates risk factors from market data."""

    def calculate_factors(
            self, asset: CryptoAsset, snapshots: List[MarketSnapshot]
    ) -> List[RiskFactor]:
        """
        Computes standardized risk metrics (Sharpe, Sortino, VaR, etc.).
        Applies z-score normalization and winsorizing.
        """


@runtime_checkable
class POmegaIntegratorPort(Protocol):
    """Integrates quantitative and sentiment factors into P_ω score."""

    def integrate(
            self,
            quant_factors: List[RiskFactor],
            sentiment_score: float,
            omega: float
    ) -> POmegaScore:
        """
        Computes P_ω = (1-ω) * quant_score + ω * sentiment_norm.
        Includes sensitivity analysis.
        """


@runtime_checkable
class PersistencePort(Protocol):
    """Persists domain entities and audit trails to DuckDB."""

    def save_article(self, article: Article) -> None:
        ...

    def save_pomega_score(self, score: POmegaScore, observed_at: datetime) -> None:
        """
        Saves profile with audit trail (votes, rejections, summaries).
        """

    def query_audit_trail(self, asset: CryptoAsset) -> Dict[str, Any]:
        """
        Retrieves audit data for transparency (CSV-export ready).
        """