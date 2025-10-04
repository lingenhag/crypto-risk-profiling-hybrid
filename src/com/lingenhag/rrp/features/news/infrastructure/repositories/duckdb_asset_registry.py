# src/com/lingenhag/rrp/features/news/infrastructure/repositories/duckdb_asset_registry.py
from __future__ import annotations

from typing import Sequence

import duckdb

from com.lingenhag.rrp.features.news.application.ports_asset_registry import AssetRegistryPort


class DuckDBAssetRegistryRepository(AssetRegistryPort):
    """
    Liest Aliases und Negativ-Begriffe für die News-Suche aus DuckDB.
    Erwartet Tabellen:
      - asset_aliases(symbol TEXT, alias TEXT)
      - asset_negative_terms(symbol TEXT, term TEXT)
    """
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(self.db_path)
        try:
            con.execute("SET TimeZone='UTC'")
        except Exception:
            pass
        return con

    def get_aliases(self, asset_symbol: str) -> Sequence[str]:
        sym = (asset_symbol or "").upper()
        with self._connect() as con:
            try:
                rows = con.execute(
                    "SELECT alias FROM asset_aliases WHERE UPPER(symbol) = ?",
                    (sym,),
                ).fetchall()
            except duckdb.CatalogException:
                # Tabelle (noch) nicht vorhanden → leer zurückgeben
                return []
        return [r[0] for r in rows if r and r[0]]

    def get_negative_terms(self, asset_symbol: str) -> Sequence[str]:
        sym = (asset_symbol or "").upper()
        with self._connect() as con:
            try:
                rows = con.execute(
                    "SELECT term FROM asset_negative_terms WHERE UPPER(symbol) = ?",
                    (sym,),
                ).fetchall()
            except duckdb.CatalogException:
                return []
        return [r[0] for r in rows if r and r[0]]