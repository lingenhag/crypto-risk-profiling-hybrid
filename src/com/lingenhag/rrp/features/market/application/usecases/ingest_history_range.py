# src/com/lingenhag/rrp/features/market/application/usecases/ingest_history_range.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List

from com.lingenhag.rrp.domain.models import DailyCandle, MarketSnapshot
from com.lingenhag.rrp.features.market.application.ports import MarketDataPort, MarketRepositoryPort


@dataclass(frozen=True)
class IngestHistoryResult:
    fetched: int
    saved: int
    duplicates: int


class IngestHistoryRange:
    """
    Holt Intraday-Snapshots (z. B. stündlich) vom Provider, persistiert diese in
    market_snapshots und rollt sie anschliessend zu Daily-Candles (market_history) auf.

    WICHTIG:
    CoinGecko liefert uns in der History-Range-API nur die provider_id (z. B. 'bitcoin').
    Unser DB-Schema referenziert aber 'assets(symbol)' per FK (z. B. 'BTC').
    Darum normalisieren wir die gelesenen Snapshots hier explizit auf das
    übergebene 'asset_symbol', bevor wir sie speichern.
    """

    def __init__(self, repo: MarketRepositoryPort, source: MarketDataPort) -> None:
        self.repo = repo
        self.source = source

    def execute(
            self,
            asset_symbol: str,
            provider_id: str,
            from_ts: int,
            to_ts: int,
            vs_currency: str = "usd",
    ) -> IngestHistoryResult:
        # 1) Intraday-Snapshots vom Provider
        raw_snapshots = self.source.fetch_history_range(
            provider_id=provider_id,
            vs_currency=vs_currency,
            ts_from=from_ts,
            ts_to=to_ts,
            granularity="hourly",
        )

        # 2) Asset-Symbol auf unser FK-Ziel normalisieren (z. B. 'BTC' statt 'BITCOIN')
        snapshots: List[MarketSnapshot] = [
            MarketSnapshot(
                asset_symbol=asset_symbol,  # <<— hier der Fix: erzwinge das echte Symbol
                price=s.price,
                market_cap=s.market_cap,
                volume_24h=s.volume_24h,
                change_1h=s.change_1h,
                change_24h=s.change_24h,
                change_7d=s.change_7d,
                observed_at=s.observed_at,
                source=s.source,
            )
            for s in raw_snapshots
        ]

        # 3) Snapshots persistieren (für Debugging/Backtests/QA)
        self.repo.upsert_snapshots(snapshots)

        # 4) Rollup → Daily-Candles und persistieren
        candles = self._to_daily_candles(snapshots, asset_symbol, provider_id, vs_currency)
        inserted, updated = self.repo.upsert_candles(candles)

        return IngestHistoryResult(
            fetched=len(snapshots),
            saved=inserted + updated,
            duplicates=0,  # Upserts behandeln Duplikate intern
        )

    def _to_daily_candles(
            self, snapshots: List[MarketSnapshot], asset_symbol: str, provider_id: str, vs_currency: str
    ) -> List[DailyCandle]:
        daily_data: dict[date, dict] = {}
        for snapshot in snapshots:
            day = snapshot.observed_at.date()
            if day not in daily_data:
                daily_data[day] = {
                    "open": snapshot.price,
                    "high": snapshot.price,
                    "low": snapshot.price,
                    "close": snapshot.price,
                    "market_cap": snapshot.market_cap,
                    "volume": snapshot.volume_24h,
                    "count": 1,
                }
            else:
                data = daily_data[day]
                data["high"] = max((data["high"] or 0.0), (snapshot.price or 0.0))
                data["low"] = min((data["low"] or float("inf")), (snapshot.price or float("inf")))
                data["close"] = snapshot.price
                # Markt-Kap. und Volumen konservativ zusammenführen
                data["market_cap"] = snapshot.market_cap or data["market_cap"]
                data["volume"] = (data["volume"] or 0.0) + (snapshot.volume_24h or 0.0)
                data["count"] += 1

        candles = [
            DailyCandle(
                asset_symbol=asset_symbol,
                provider="CoinGecko",
                provider_id=provider_id,
                vs_currency=vs_currency,
                day=d,
                open=vals["open"],
                high=vals["high"],
                low=vals["low"],
                close=vals["close"],
                market_cap=vals["market_cap"],
                volume=(vals["volume"] / vals["count"]) if vals["count"] > 0 else None,
                source="CoinGecko",
            )
            for d, vals in daily_data.items()
        ]
        return candles