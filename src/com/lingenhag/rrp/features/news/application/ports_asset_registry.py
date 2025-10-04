# src/com/lingenhag/rrp/features/news/application/ports_asset_registry.py
from __future__ import annotations

from typing import Protocol, Sequence


class AssetRegistryPort(Protocol):
    """
    Liefert Such-Aliases und Negative-Filter für Assets.
    Infrastructure-Adapter (z. B. DuckDB) implementieren dieses Protokoll.
    """
    def get_aliases(self, asset_symbol: str) -> Sequence[str]:
        ...

    def get_negative_terms(self, asset_symbol: str) -> Sequence[str]:
        ...