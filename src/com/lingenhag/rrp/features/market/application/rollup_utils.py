# src/com/lingenhag/rrp/features/market/application/rollup_utils.py
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Iterable, List

from com.lingenhag.rrp.domain.models import DailyCandle, MarketSnapshot


def _utc_date(ts: datetime) -> date:
    """Normalisiert auf UTC und gibt Datum zurück."""
    return ts.astimezone(timezone.utc).date()


def rollup_daily_candles(
        snapshots: Iterable[MarketSnapshot],
        *,
        asset_symbol: str,
        provider: str,
        provider_id: str,
        vs_currency: str,
) -> List[DailyCandle]:
    """
    Aggregiert Intraday-Snapshots zu Daily-Candles (OHLC + Market Cap + Volume).

    Regeln:
      - open/close: erster/letzter Preis des Tages
      - high/low  : Max/Min der Preise des Tages
      - market_cap: letzter bekannter Wert des Tages
      - volume    : Summe der Intraday-"volume_24h" (konservativ)
    """
    # Buckets pro Tag
    buckets: dict[date, list[MarketSnapshot]] = {}
    for s in snapshots:
        d = _utc_date(s.observed_at)
        buckets.setdefault(d, []).append(s)

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
                vs_currency=vs_currency,
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

    candles.sort(key=lambda c: c.day)
    return candles