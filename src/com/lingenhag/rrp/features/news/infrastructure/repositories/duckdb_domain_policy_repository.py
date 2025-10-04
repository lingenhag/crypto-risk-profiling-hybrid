# src/com/lingenhag/rrp/features/news/infrastructure/repositories/duckdb_domain_policy_repository.py
from __future__ import annotations

import duckdb
from datetime import datetime, timezone


class DuckDBDomainPolicyRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._ensure_schema()

    def _connect(self):
        return duckdb.connect(self.db_path)

    def _ensure_schema(self) -> None:
        with self._connect() as con:
            # Policy-Tabelle
            con.execute("""
                        CREATE TABLE IF NOT EXISTS news_domain_policy
                        (
                            asset_symbol TEXT NOT NULL,
                            domain       TEXT NOT NULL,
                            allowed      BOOLEAN NOT NULL DEFAULT TRUE,
                            created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            CONSTRAINT pk_news_domain_policy PRIMARY KEY (asset_symbol, domain)
                            )
                        """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_domain_policy_asset   ON news_domain_policy(asset_symbol)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_domain_policy_allowed ON news_domain_policy(allowed)")

            # Stats-Tabelle (split: harvested/stored vs. llm_accepted/llm_rejected)
            con.execute("""
                        CREATE TABLE IF NOT EXISTS news_domain_stats
                        (
                            asset_symbol     TEXT NOT NULL,
                            domain           TEXT NOT NULL,
                            harvested_total  BIGINT NOT NULL DEFAULT 0,
                            stored_total     BIGINT NOT NULL DEFAULT 0,
                            llm_accepted     BIGINT NOT NULL DEFAULT 0,
                            llm_rejected     BIGINT NOT NULL DEFAULT 0,
                            CONSTRAINT pk_news_domain_stats PRIMARY KEY (asset_symbol, domain)
                            )
                        """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_domain_stats_asset ON news_domain_stats(asset_symbol)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_domain_stats_hv    ON news_domain_stats(harvested_total, stored_total)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_domain_stats_llm   ON news_domain_stats(llm_accepted, llm_rejected)")

    # ---------- Policy ----------
    def is_allowed(self, asset_symbol: str, domain: str) -> bool:
        try:
            with self._connect() as con:
                row = con.execute(
                    "SELECT allowed FROM news_domain_policy WHERE asset_symbol = ? AND domain = ?",
                    (asset_symbol, domain),
                ).fetchone()
                return True if row is None else bool(row[0])
        except duckdb.Error:
            # Fail-open
            return True

    def allow(self, asset_symbol: str, domain: str, allowed: bool = True) -> None:
        with self._connect() as con:
            now = self._now()
            con.execute(
                """
                INSERT INTO news_domain_policy (asset_symbol, domain, allowed, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (asset_symbol, domain) DO UPDATE
                                                              SET allowed = excluded.allowed, updated_at = excluded.updated_at
                """,
                (asset_symbol, domain, allowed, now, now),
            )

    # ---------- Stats (low-level) ----------
    def bump_harvested(self, asset_symbol: str, domain: str, by: int = 1) -> None:
        self._upsert_counter(asset_symbol, domain, "harvested_total", by)

    def bump_stored(self, asset_symbol: str, domain: str, by: int = 1) -> None:
        self._upsert_counter(asset_symbol, domain, "stored_total", by)

    def bump_llm_accepted(self, asset_symbol: str, domain: str, by: int = 1) -> None:
        self._upsert_counter(asset_symbol, domain, "llm_accepted", by)

    def bump_llm_rejected(self, asset_symbol: str, domain: str, by: int = 1) -> None:
        self._upsert_counter(asset_symbol, domain, "llm_rejected", by)

    def _upsert_counter(self, asset_symbol: str, domain: str, col: str, by: int) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO news_domain_stats (asset_symbol, domain, harvested_total, stored_total, llm_accepted, llm_rejected)
                VALUES (?, ?, 0, 0, 0, 0)
                    ON CONFLICT (asset_symbol, domain) DO NOTHING
                """,
                (asset_symbol, domain),
            )
            con.execute(
                f"UPDATE news_domain_stats SET {col} = {col} + ? WHERE asset_symbol = ? AND domain = ?",
                (by, asset_symbol, domain),
            )

    # ---------- Stats (high-level, NEU) ----------
    def record_harvest(self, asset_symbol: str, domain: str, *, stored: bool) -> None:
        """Immer harvested_total +1; zusätzlich stored_total +1 wenn stored=True."""
        self.bump_harvested(asset_symbol, domain, 1)
        if stored:
            self.bump_stored(asset_symbol, domain, 1)

    def record_llm_decision(self, asset_symbol: str, domain: str, *, accepted: bool) -> None:
        """LLM-Entscheidungen verbuchen."""
        if accepted:
            self.bump_llm_accepted(asset_symbol, domain, 1)
        else:
            self.bump_llm_rejected(asset_symbol, domain, 1)

    @staticmethod
    def _now():
        return datetime.now(timezone.utc)