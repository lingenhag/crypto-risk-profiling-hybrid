# src/com/lingenhag/rrp/features/market/application/usecases/market_history_job.py
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Iterable, List, Optional

from com.lingenhag.rrp.domain.models import MarketSnapshot, DailyCandle
from com.lingenhag.rrp.features.market.application.ports import MarketDataPort, MarketRepositoryPort


@dataclass(frozen=True)
class SeedConfig:
    years: int = 5
    vs_currency: str = "usd"
    provider: str = "CoinGecko"
    granularity: str = "hourly"  # konfigurierbar ("hourly", "daily")


def _utc_date(ts: datetime) -> date:
    return ts.astimezone(timezone.utc).date()


def _rollup_daily(
        snaps: Iterable[MarketSnapshot],
        *,
        asset_symbol: str,
        provider: str,
        provider_id: str,
        vs: str,
) -> List[DailyCandle]:
    buckets: dict[date, list[MarketSnapshot]] = defaultdict(list)
    for s in snaps:
        buckets[_utc_date(s.observed_at)].append(s)
    candles: list[DailyCandle] = []
    for d, arr in buckets.items():
        arr_sorted = sorted(arr, key=lambda x: x.observed_at)
        prices = [x.price for x in arr_sorted if x.price is not None]
        open_p = prices[0] if prices else None
        close_p = prices[-1] if prices else None
        high_p = max(prices) if prices else None
        low_p = min(prices) if prices else None
        # market cap: letzter bekannter Wert des Tages
        mcap = next((x.market_cap for x in reversed(arr_sorted) if x.market_cap is not None), None)
        # volume: Summe der Intraday-Volumes
        vol_sum = sum((x.volume_24h or 0.0) for x in arr_sorted)
        candles.append(
            DailyCandle(
                asset_symbol=asset_symbol,
                provider=provider,
                provider_id=provider_id,
                vs_currency=vs,
                day=d,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                market_cap=mcap,
                volume=vol_sum,
                source=provider,
            )
        )
    return sorted(candles, key=lambda c: c.day)


class MarketHistoryJob:
    def __init__(self, repo: MarketRepositoryPort, cg: MarketDataPort):
        self._repo = repo
        self._cg = cg

    def seed_initial(self, *, asset_symbol: str, provider_id: str, cfg: SeedConfig = SeedConfig()) -> tuple[int, int]:
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=cfg.years * 365)
        snaps = self._cg.fetch_history_range(
            provider_id=provider_id,
            vs_currency=cfg.vs_currency,
            ts_from=int(start.timestamp()),
            ts_to=int(now.timestamp()),
            granularity=cfg.granularity,
        )
        candles = _rollup_daily(
            snaps,
            asset_symbol=asset_symbol,
            provider=cfg.provider,
            provider_id=provider_id,
            vs=cfg.vs_currency,
        )
        return self._repo.upsert_candles(candles)

    def update_incremental(
            self,
            *,
            asset_symbol: str,
            provider_id: str,
            vs_currency: str = "usd",
            provider: str = "CoinGecko",
            granularity: str = "hourly",
    ) -> tuple[int, int]:
        last = self._repo.last_stored_day(asset_symbol=asset_symbol, provider=provider, vs_currency=vs_currency)
        if last is None:
            return 0, 0
        start_dt = datetime.combine(last + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if start_dt.date() > now.date():
            return 0, 0
        snaps = self._cg.fetch_history_range(
            provider_id=provider_id,
            vs_currency=vs_currency,
            ts_from=int(start_dt.timestamp()),
            ts_to=int(now.timestamp()),
            granularity=granularity,
        )
        candles = _rollup_daily(
            snaps,
            asset_symbol=asset_symbol,
            provider=provider,
            provider_id=provider_id,
            vs=vs_currency,
        )
        return self._repo.upsert_candles(candles)