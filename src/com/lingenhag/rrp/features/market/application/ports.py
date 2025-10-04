# src/com/lingenhag/rrp/features/market/application/ports.py
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Protocol, Tuple

from com.lingenhag.rrp.domain.models import DailyCandle, MarketSnapshot


class MarketDataPort(Protocol):
    def fetch_spot(self, provider_ids: List[str], vs_currency: str) -> List[MarketSnapshot]:
        ...

    def fetch_history_range(
            self,
            provider_id: str,
            vs_currency: str,
            ts_from: int,
            ts_to: int,
            granularity: str,
    ) -> List[MarketSnapshot]:
        ...


class MarketRepositoryPort(Protocol):
    # Snapshots (intraday/raw)
    def upsert_snapshots(self, snapshots: List[MarketSnapshot]) -> tuple[int, int]:
        ...

    # Candles (daily/rollup)
    def upsert_candles(self, candles: List[DailyCandle]) -> tuple[int, int]:
        ...

    def last_stored_day(self, asset_symbol: str, provider: str, vs_currency: str) -> Optional[date]:
        ...

    def fetch_range(
            self,
            asset_symbol: str,
            provider: str,
            vs_currency: str,
            start: date,
            end: date,
    ) -> List[DailyCandle]:
        ...

    def get_provider_id(self, asset_symbol: str, provider: str) -> Optional[str]:
        ...

    def upsert_asset_provider(self, asset_symbol: str, provider: str, provider_id: str) -> None:
        ...

    def list_provider_pairs(self, provider: str, asset_symbols: List[str]) -> List[tuple[str, str]]:
        ...

    # ---- Factors/Sentiment ----
    def fetch_daily_returns(self, asset_symbol: str, start: date, end: date) -> List[Tuple[date, Optional[float]]]:
        """Liefert (date, ret_1d) aus v_daily_returns für den Zeitraum."""
        ...

    def fetch_daily_sentiment(self, asset_symbol: str, start: date, end: date) -> Dict[date, Optional[float]]:
        """Mapping {date -> avg_sentiment} aus v_daily_sentiment."""
        ...

    def upsert_market_factors(self, rows: List[object]) -> tuple[int, int]:
        """
        Persistiert market_factors_daily Zeilen.
        rows-Elemente können dicts oder Objekte mit gleichnamigen Attributen sein:
          required: asset_symbol, date (oder day)
          optional: ret_1d, vol_30d, sharpe_30d, exp_return_30d,
                    sentiment_mean, sentiment_norm, p_alpha, alpha,
                    sortino_30d, var_1d_95
        Rückgabe: (inserted, updated)
        """
        ...

    def upsert_factors(self, rows: List[object]) -> tuple[int, int]:
        """Alias für upsert_market_factors(), Backwards-Compat."""
        ...


class SeedingRepositoryPort(Protocol):
    """
    Minimales Seeding-Interface für Demo-/Testdaten über DuckDB.
    Kapselt Cross-Slice-Schreibzugriffe (assets, summarized_articles, news_domain_stats).
    """

    # Assets / Provider
    def ensure_asset(self, symbol: str, name: str) -> None: ...
    def ensure_asset_provider(self, symbol: str, provider: str, provider_id: str) -> None: ...

    # Cleanup (idempotenzfreundlich)
    def delete_market_history_range(self, symbol: str, start: date, end: date) -> int: ...
    def delete_articles_range(self, symbol: str, start: date, end: date) -> int: ...
    def reset_domain_stats(self, symbol: str) -> int: ...

    # Summarized articles
    def bulk_insert_articles(
            self,
            rows: List[Tuple[str, datetime, str, str, str, str, float]],
    ) -> int:
        """
        rows: (url, published_at, summary, asset_symbol, source, model, sentiment)
        Rückgabe: Anzahl eingefügter Zeilen.
        """
        ...

    # Domain-Stats
    def upsert_news_domain_stats(
            self,
            symbol: str,
            domain_counts: Dict[str, int],
            accepted_counts: Dict[str, int],
    ) -> int:
        """
        Aktualisiert news_domain_stats: harvested_total, stored_total, llm_accepted.
        Rückgabe: Anzahl betroffener Domains.
        """
        ...