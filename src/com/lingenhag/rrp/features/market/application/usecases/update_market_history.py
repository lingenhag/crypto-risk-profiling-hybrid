# src/com/lingenhag/rrp/features/market/application/usecases/update_market_history.py
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from com.lingenhag.rrp.features.market.application.usecases.ingest_history_range import IngestHistoryRange
from com.lingenhag.rrp.features.market.application.ports import MarketRepositoryPort, MarketDataPort


class UpdateMarketHistory:
    def __init__(self, repo: MarketRepositoryPort, source: MarketDataPort) -> None:
        self.repo = repo
        self.source = source
        self.ingest = IngestHistoryRange(repo=repo, source=source)

    def execute(self, asset_symbol: str, vs_currency: str = "usd") -> None:
        last_day = self.repo.last_stored_day(asset_symbol=asset_symbol, provider="CoinGecko", vs_currency=vs_currency)
        start_date = last_day + timedelta(days=1) if last_day else date.today() - timedelta(days=30)
        end_date = date.today()

        provider_id = self.repo.get_provider_id(asset_symbol=asset_symbol, provider="CoinGecko") or asset_symbol.lower()
        from_ts = int(datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        to_ts = int(datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc).timestamp())

        self.ingest.execute(
            asset_symbol=asset_symbol,
            provider_id=provider_id,
            from_ts=from_ts,
            to_ts=to_ts,
            vs_currency=vs_currency,
        )