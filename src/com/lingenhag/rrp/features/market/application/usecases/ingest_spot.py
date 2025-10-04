# src/com/lingenhag/rrp/features/market/application/usecases/ingest_spot.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from com.lingenhag.rrp.features.market.application.ports import MarketDataPort, MarketRepositoryPort


@dataclass(frozen=True)
class IngestSpotResult:
    requested: int
    fetched: int
    saved: int
    duplicates: int


class IngestSpot:
    def __init__(self, repo: MarketRepositoryPort, source: MarketDataPort) -> None:
        self.repo = repo
        self.source = source

    def execute(self, assets: List[Tuple[str, str]], vs_currency: str = "usd") -> IngestSpotResult:
        if not assets:
            return IngestSpotResult(requested=0, fetched=0, saved=0, duplicates=0)

        provider_ids = [pid for _, pid in assets]
        snapshots = self.source.fetch_spot(provider_ids=provider_ids, vs_currency=vs_currency)

        inserted, dupes = self.repo.upsert_snapshots(snapshots)

        return IngestSpotResult(
            requested=len(assets),
            fetched=len(snapshots),
            saved=inserted,
            duplicates=dupes,
        )