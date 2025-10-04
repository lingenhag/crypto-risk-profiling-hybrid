# src/com/lingenhag/rrp/features/market/application/usecases/dashboard_queries.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from com.lingenhag.rrp.features.market.application.ports import MarketRepositoryPort


@dataclass(frozen=True)
class MarketOverview:
    asset_symbol: str
    latest_close: float
    avg_volume: float
    avg_market_cap: float


class DashboardQueries:
    def __init__(self, repo: MarketRepositoryPort) -> None:
        self.repo = repo

    def market_overview(self, asset_symbol: str, start: date, end: date) -> MarketOverview:
        candles = self.repo.fetch_range(
            asset_symbol=asset_symbol,
            provider="CoinGecko",
            vs_currency="usd",
            start=start,
            end=end,
        )
        if not candles:
            return MarketOverview(asset_symbol=asset_symbol, latest_close=0.0, avg_volume=0.0, avg_market_cap=0.0)

        latest_close = candles[-1].close or 0.0
        avg_volume = sum(c.volume or 0.0 for c in candles) / len(candles) if any(c.volume for c in candles) else 0.0
        avg_market_cap = (
            sum(c.market_cap or 0.0 for c in candles) / len(candles) if any(c.market_cap for c in candles) else 0.0
        )

        return MarketOverview(
            asset_symbol=asset_symbol,
            latest_close=latest_close,
            avg_volume=avg_volume,
            avg_market_cap=avg_market_cap,
        )