# src/com/lingenhag/rrp/features/news/infrastructure/repositories/duckdb_news_repository.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from com.lingenhag.rrp.domain.models import UrlHarvest, SummarizedArticle
from ...application.ports import NewsRepositoryPort


logger = logging.getLogger(__name__)


class DuckDBNewsRepository(NewsRepositoryPort):
    """
    Persistence adapter for news using DuckDB.
    - Stores all timestamps as **UTC-naive** TIMESTAMP (session TZ = UTC).
    - Normalizes incoming aware datetimes to UTC and strips tzinfo.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    # ----------------------------------
    # Internal
    # ----------------------------------
    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(self.db_path)
        try:
            # Critical: Set session timezone to UTC for naive TIMESTAMP interpretation.
            con.execute("SET TimeZone='UTC'")
        except Exception as e:
            logger.warning(f"Failed to set UTC timezone: {e}")
        return con

    @staticmethod
    def _to_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            # Assumption: already UTC-naive
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    # ----------------------------------
    # Port Implementations
    # ----------------------------------
    def save_url_harvest(
            self,
            *,
            url: str,
            asset_symbol: str,
            source: Optional[str],
            published_at: Optional[datetime],
            title: Optional[str],
    ) -> Tuple[int, bool]:
        """
        Saves a new harvest entry if not already existing in:
        (a) url_harvests or
        (b) processed/rejected (summarized_articles | rejections).

        Returns:
            (id, is_duplicate)
            - id: ID of inserted url_harvests entry; 0 if duplicate.
            - is_duplicate: True if (a) or (b) applies; else False.
        """
        with self._connect() as con:
            try:
                con.begin()  # Start transaction for atomicity

                # (b) Already processed (summarized_articles) or rejected?
                #     -> Do not re-add to inbox.
                already_processed = con.execute(
                    """
                    SELECT 1 FROM summarized_articles
                    WHERE url = ? AND asset_symbol = ?
                        LIMIT 1
                    """,
                    (url, asset_symbol),
                ).fetchone()

                already_rejected = None
                if already_processed is None:
                    already_rejected = con.execute(
                        """
                        SELECT 1 FROM rejections
                        WHERE url = ? AND asset_symbol = ?
                            LIMIT 1
                        """,
                        (url, asset_symbol),
                    ).fetchone()

                if already_processed is not None or already_rejected is not None:
                    # Already known (processed or rejected) -> count as duplicate.
                    return 0, True

                # (a) Already in url_harvests?
                existing = con.execute(
                    "SELECT id FROM url_harvests WHERE url = ? AND asset_symbol = ?",
                    (url, asset_symbol),
                ).fetchone()
                if existing:
                    return existing[0], True

                # Insert new
                pa = self._to_utc_naive(published_at)
                discovered = self._to_utc_naive(datetime.now(timezone.utc))

                row = con.execute(
                    """
                    INSERT INTO url_harvests
                        (url, asset_symbol, source, published_at, title, discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                        RETURNING id
                    """,
                    (url, asset_symbol, source, pa, title, discovered),
                ).fetchone()
                con.commit()
                return row[0], False

            except Exception as e:
                con.rollback()
                logger.error(f"Failed to save URL harvest for {url}: {e}")
                raise

    def save_summarized_article(self, article: SummarizedArticle) -> int:
        """
        Saves a summarized article with sentiment.
        Returns: Inserted ID.
        """
        with self._connect() as con:
            try:
                con.begin()
                ingested = self._to_utc_naive(article.ingested_at or datetime.now(timezone.utc))
                pa = self._to_utc_naive(article.published_at)

                row = con.execute(
                    """
                    INSERT INTO summarized_articles
                    (url, published_at, summary, asset_symbol, source, model, sentiment, ingested_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        RETURNING id
                    """,
                    (
                        article.url,
                        pa,
                        article.summary,
                        article.asset_symbol,
                        article.source,
                        article.model,
                        article.sentiment,
                        ingested,
                    ),
                ).fetchone()
                con.commit()
                return row[0]

            except Exception as e:
                con.rollback()
                logger.error(f"Failed to save summarized article {article.url}: {e}")
                raise

    def save_rejection(
            self,
            *,
            url: str,
            asset_symbol: str,
            reason: str,
            source: Optional[str],
            context: str,
            article_id: Optional[int] = None,
    ) -> int:
        """
        Saves a rejection entry for audit trail.
        Returns: Inserted ID.
        """
        with self._connect() as con:
            try:
                con.begin()
                created = self._to_utc_naive(datetime.now(timezone.utc))
                row = con.execute(
                    """
                    INSERT INTO rejections
                        (url, asset_symbol, reason, source, context, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                        RETURNING id
                    """,
                    (url, asset_symbol, reason, source, context, created),
                ).fetchone()
                con.commit()
                return row[0]

            except Exception as e:
                con.rollback()
                logger.error(f"Failed to save rejection for {url}: {e}")
                raise

    def now_utc(self) -> datetime:
        """Returns current UTC datetime (aware)."""
        return datetime.now(timezone.utc)

    def parse_datetime(self, value: Optional[Any]) -> Optional[datetime]:
        """
        Robust parsing for DuckDB returns:
        - datetime -> normalize to UTC (aware)
        - str -> ISO parsing, 'Z' to '+00:00'
        - None/else -> None
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                logger.warning(f"Failed to parse datetime string: {value}")
                return None
        return None

    def fetch_url_harvest_batch(
            self,
            asset_symbol: str,
            limit: int = 10,
            since_utc: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:  # Dict for test assert
        """
        Fetches batch of pending URL harvests as dicts.
        Returns: List of harvest dicts.
        """
        with self._connect() as con:
            params: List[Any] = [asset_symbol]
            query = """
                    SELECT id, url, asset_symbol, source, published_at, title, discovered_at
                    FROM url_harvests
                    WHERE asset_symbol = ?
                    """
            if since_utc is not None:
                query += " AND discovered_at >= ?"
                params.append(self._to_utc_naive(since_utc))
            query += " ORDER BY discovered_at ASC LIMIT ?"
            params.append(int(limit))

            rows = con.execute(query, params).fetchall()

            harvests = []
            for row in rows:
                published_at = self.parse_datetime(row[4])
                discovered_at = self.parse_datetime(row[6])
                harvest = {
                    "id": row[0],
                    "url": row[1],
                    "asset_symbol": row[2],
                    "source": row[3],
                    "published_at": published_at,
                    "title": row[5],
                    "discovered_at": discovered_at,
                }
                harvests.append(harvest)
            return harvests

    def fetch_rejections(self, asset_symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Fetches recent rejections for audit trail.
        Returns: List of rejection dicts.
        """
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT id, url, reason, source, context, created_at
                FROM rejections
                WHERE asset_symbol = ?
                ORDER BY created_at DESC
                    LIMIT ?
                """,
                (asset_symbol, int(limit)),
            ).fetchall()

            return [
                {
                    "id": row[0],
                    "url": row[1],
                    "reason": row[2],
                    "source": row[3],
                    "context": row[4],
                    "created_at": self.parse_datetime(row[5]),
                }
                for row in rows
            ]

    def delete_url_harvest(self, harvest_id: int) -> None:
        """Deletes a processed URL harvest."""
        with self._connect() as con:
            try:
                con.begin()
                con.execute("DELETE FROM url_harvests WHERE id = ?", (harvest_id,))
                con.commit()
            except Exception as e:
                con.rollback()
                logger.error(f"Failed to delete URL harvest {harvest_id}: {e}")
                raise