# src/com/lingenhag/rrp/features/market/infrastructure/repositories/duckdb_market_repository.py
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import duckdb

from com.lingenhag.rrp.domain.models import DailyCandle, MarketSnapshot
from com.lingenhag.rrp.features.market.application.ports import MarketRepositoryPort


def _ts_utc_naive(dt: datetime | date) -> datetime | date:
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _get(field: str, row: object, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(field, default)
    return getattr(row, field, default)


class DuckDBMarketRepository(MarketRepositoryPort):
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(self.db_path)
        try:
            con.execute("SET TimeZone='UTC'")
        except Exception:
            pass
        return con

    def _ensure_table(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        if not info:
            raise RuntimeError(f"{table} fehlt. Migration ausführen.")

    # -------- Snapshots (intraday) --------
    def upsert_snapshots(self, snapshots: Sequence[MarketSnapshot]) -> Tuple[int, int]:
        if not snapshots:
            return 0, 0
        inserted = 0
        skipped = 0
        with self._connect() as con:
            self._ensure_table(con, "market_snapshots")
            for s in snapshots:
                row = con.execute(
                    """
                    SELECT 1 FROM market_snapshots
                    WHERE asset_symbol=? AND observed_at=? AND source=?
                    """,
                    [
                        s.asset_symbol,
                        _ts_utc_naive(s.observed_at),
                        s.source or "CoinGecko",
                        ],
                ).fetchone()
                if row:
                    skipped += 1
                    continue
                con.execute(
                    """
                    INSERT INTO market_snapshots
                    (asset_symbol, price, market_cap, volume_24h, change_1h, change_24h, change_7d, observed_at, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        s.asset_symbol,
                        s.price,
                        s.market_cap,
                        s.volume_24h,
                        s.change_1h,
                        s.change_24h,
                        s.change_7d,
                        _ts_utc_naive(s.observed_at),
                        s.source or "CoinGecko",
                        ],
                )
                inserted += 1
        return inserted, skipped

    # -------- Candles (daily) --------
    def upsert_candles(self, candles: Sequence[DailyCandle]) -> Tuple[int, int]:
        if not candles:
            return 0, 0
        inserted = 0
        updated = 0
        with self._connect() as con:
            self._ensure_table(con, "market_history")
            for c in candles:
                exists = con.execute(
                    """
                    SELECT 1 FROM market_history
                    WHERE asset_symbol=? AND provider=? AND vs_currency=? AND date=?
                    """,
                    [c.asset_symbol, c.provider, c.vs_currency, c.day],
                ).fetchone()
                if exists:
                    con.execute(
                        """
                        UPDATE market_history
                        SET open=COALESCE(?, open),
                            high=COALESCE(?, high),
                            low=COALESCE(?, low),
                            close=COALESCE(?, close),
                            market_cap=COALESCE(?, market_cap),
                            volume=COALESCE(?, volume),
                            source=COALESCE(?, source),
                            updated_at=CURRENT_TIMESTAMP
                        WHERE asset_symbol=? AND provider=? AND vs_currency=? AND date=?
                        """,
                        [
                            c.open,
                            c.high,
                            c.low,
                            c.close,
                            c.market_cap,
                            c.volume,
                            c.source,
                            c.asset_symbol,
                            c.provider,
                            c.vs_currency,
                            c.day,
                        ],
                    )
                    updated += 1
                else:
                    con.execute(
                        """
                        INSERT INTO market_history
                        (asset_symbol, provider, provider_id, vs_currency, date,
                         open, high, low, close, market_cap, volume, source, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        [
                            c.asset_symbol,
                            c.provider,
                            c.provider_id,
                            c.vs_currency,
                            c.day,
                            c.open,
                            c.high,
                            c.low,
                            c.close,
                            c.market_cap,
                            c.volume,
                            c.source,
                        ],
                    )
                    inserted += 1
        return inserted, updated

    def last_stored_day(self, asset_symbol: str, provider: str, vs_currency: str) -> Optional[date]:
        with self._connect() as con:
            self._ensure_table(con, "market_history")
            row = con.execute(
                """
                SELECT max(date) FROM market_history
                WHERE asset_symbol=? AND provider=? AND vs_currency=?
                """,
                [asset_symbol, provider, vs_currency],
            ).fetchone()
            return row[0] if row and row[0] is not None else None

    def fetch_range(
            self, asset_symbol: str, provider: str, vs_currency: str, start: date, end: date
    ) -> List[DailyCandle]:
        with self._connect() as con:
            self._ensure_table(con, "market_history")
            rows = con.execute(
                """
                SELECT asset_symbol, provider, provider_id, vs_currency, date,
                    open, high, low, close, market_cap, volume, source
                FROM market_history
                WHERE asset_symbol=? AND provider=? AND vs_currency=? AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """,
                [asset_symbol, provider, vs_currency, start, end],
            ).fetchall()
        return [
            DailyCandle(
                asset_symbol=r[0],
                provider=r[1],
                provider_id=r[2],
                vs_currency=r[3],
                day=r[4],
                open=r[5],
                high=r[6],
                low=r[7],
                close=r[8],
                market_cap=r[9],
                volume=r[10],
                source=r[11],
            )
            for r in rows
        ]

    def get_provider_id(self, asset_symbol: str, provider: str) -> Optional[str]:
        with self._connect() as con:
            self._ensure_table(con, "asset_providers")
            row = con.execute(
                """
                SELECT provider_id FROM asset_providers
                WHERE asset_symbol=? AND provider=?
                """,
                [asset_symbol, provider],
            ).fetchone()
            return row[0] if row else None

    def upsert_asset_provider(self, asset_symbol: str, provider: str, provider_id: str) -> None:
        with self._connect() as con:
            self._ensure_table(con, "asset_providers")
            con.execute(
                """
                INSERT OR IGNORE INTO asset_providers (asset_symbol, provider, provider_id)
                VALUES (?, ?, ?)
                """,
                [asset_symbol, provider, provider_id],
            )
            con.execute(
                """
                UPDATE asset_providers
                SET provider_id=?
                WHERE asset_symbol=? AND provider=?
                """,
                [provider_id, asset_symbol, provider],
            )

    def list_provider_pairs(self, provider: str, asset_symbols: List[str]) -> List[Tuple[str, str]]:
        """
        Liefert [(asset_symbol, provider_id)] für gegebene Symbols.
        Fix für DuckDB: UNNEST-Parameter korrekt verwenden (Spaltenname ist 'unnest' oder via Alias).
        """
        if not asset_symbols:
            return []
        with self._connect() as con:
            self._ensure_table(con, "asset_providers")
            rows = con.execute(
                """
                SELECT asset_symbol, provider_id
                FROM asset_providers
                WHERE provider=?
                  AND asset_symbol IN (SELECT * FROM UNNEST(?))
                """,
                [provider, asset_symbols],
            ).fetchall()
        return [(row[0], row[1]) for row in rows]

    # -------- Factors & Sentiment --------
    def fetch_daily_returns(self, asset_symbol: str, start: date, end: date) -> List[Tuple[date, Optional[float]]]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT date, ret_1d
                FROM v_daily_returns
                WHERE asset_symbol=? AND date BETWEEN ? AND ?
                ORDER BY date ASC
                """,
                [asset_symbol, start, end],
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def fetch_daily_sentiment(self, asset_symbol: str, start: date, end: date) -> Dict[date, Optional[float]]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT day, avg_sentiment
                FROM v_daily_sentiment
                WHERE asset_symbol=? AND day BETWEEN ? AND ?
                ORDER BY day ASC
                """,
                [asset_symbol, start, end],
            ).fetchall()
        return {r[0]: r[1] for r in rows}

    def fetch_daily_sentiment_stats(self, asset_symbol: str, start: date, end: date) -> Dict[date, int]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT day, n_articles
                FROM v_daily_sentiment_with_counts
                WHERE asset_symbol=? AND day BETWEEN ? AND ?
                ORDER BY day ASC
                """,
                [asset_symbol, start, end],
            ).fetchall()
        return {r[0]: int(r[1]) if r[1] is not None else 0 for r in rows}

    def upsert_market_factors(self, rows: List[object]) -> tuple[int, int]:
        if not rows:
            return 0, 0
        inserted = 0
        updated = 0
        with self._connect() as con:
            self._ensure_table(con, "market_factors_daily")
            for row in rows:
                asset_symbol = _get("asset_symbol", row)
                day = _get("date", row) or _get("day", row)
                ret_1d = _get("ret_1d", row)
                vol_30d = _get("vol_30d", row)
                sharpe_30d = _get("sharpe_30d", row)
                exp_return_30d = _get("exp_return_30d", row)
                sentiment_mean = _get("sentiment_mean", row)
                sentiment_norm = _get("sentiment_norm", row)
                p_alpha = _get("p_alpha", row)
                alpha = _get("alpha", row)
                sortino_30d = _get("sortino_30d", row)
                var_1d_95 = _get("var_1d_95", row)

                exists = con.execute(
                    "SELECT 1 FROM market_factors_daily WHERE asset_symbol=? AND date=?",
                    [asset_symbol, day],
                ).fetchone()
                if exists:
                    con.execute(
                        """
                        UPDATE market_factors_daily
                        SET ret_1d=?,
                            vol_30d=?,
                            sharpe_30d=?,
                            exp_return_30d=?,
                            sentiment_mean=?,
                            sentiment_norm=?,
                            p_alpha=?,
                            alpha=?,
                            sortino_30d=?,
                            var_1d_95=?,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE asset_symbol=? AND date=?
                        """,
                        [
                            ret_1d,
                            vol_30d,
                            sharpe_30d,
                            exp_return_30d,
                            sentiment_mean,
                            sentiment_norm,
                            p_alpha,
                            alpha,
                            sortino_30d,
                            var_1d_95,
                            asset_symbol,
                            day,
                        ],
                    )
                    updated += 1
                else:
                    con.execute(
                        """
                        INSERT INTO market_factors_daily
                        (asset_symbol, date,
                         ret_1d, vol_30d, sharpe_30d, exp_return_30d,
                         sentiment_mean, sentiment_norm,
                         p_alpha, alpha, sortino_30d, var_1d_95,
                         created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        [
                            asset_symbol,
                            day,
                            ret_1d,
                            vol_30d,
                            sharpe_30d,
                            exp_return_30d,
                            sentiment_mean,
                            sentiment_norm,
                            p_alpha,
                            alpha,
                            sortino_30d,
                            var_1d_95,
                        ],
                    )
                    inserted += 1
        return inserted, updated

    def upsert_factors(self, rows: List[object]) -> tuple[int, int]:
        return self.upsert_market_factors(rows)