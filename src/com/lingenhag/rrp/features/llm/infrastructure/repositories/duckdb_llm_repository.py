# src/com/lingenhag/rrp/features/llm/infrastructure/repositories/duckdb_llm_repository.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import duckdb

from com.lingenhag.rrp.domain.models import SummarizedArticle
from com.lingenhag.rrp.features.llm.application.ports import VotesRepositoryPort


class DuckDBLLMRepository(VotesRepositoryPort):
    """
    Persistenz für LLM-Votes und Zusammenfassungen.
    Konvention: Alle TIMESTAMPs werden als UTC-naiv in DuckDB gespeichert.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self):
        con = duckdb.connect(self.db_path)
        try:
            con.execute("SET TimeZone='UTC'")
        except Exception:
            pass
        return con

    @staticmethod
    def _to_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            # Annahme: bereits UTC
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    def save_vote(
            self,
            url: str,
            asset_symbol: str,
            model: str,
            relevance: bool,
            sentiment: Optional[float],
            summary: Optional[str],
            harvest_id: Optional[int] = None,
            article_id: Optional[int] = None,
    ) -> int:
        with self._connect() as con:
            result = con.execute(
                """
                INSERT INTO llm_votes
                (url, asset_symbol, model, relevance, sentiment, summary, created_at, harvest_id, article_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                """,
                (
                    url,
                    asset_symbol,
                    model,
                    relevance,
                    sentiment,
                    summary,
                    self._to_utc_naive(datetime.now(timezone.utc)),
                    harvest_id,
                    article_id,
                ),
            )
            return result.fetchone()[0]

    def save_summary(self, article: SummarizedArticle) -> int:
        with self._connect() as con:
            result = con.execute(
                """
                INSERT INTO summarized_articles
                (url, published_at, summary, asset_symbol, source, model, sentiment, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                """,
                (
                    article.url,
                    self._to_utc_naive(article.published_at),
                    article.summary,
                    article.asset_symbol,
                    article.source,
                    article.model,
                    article.sentiment,
                    self._to_utc_naive(article.ingested_at or datetime.now(timezone.utc)),
                ),
            )
            return result.fetchone()[0]

    def save_rejection(
            self,
            url: str,
            asset_symbol: str,
            reason: str,
            source: Optional[str],
            context: str,
            article_id: Optional[int] = None,
            *,
            model: Optional[str] = None,
            details_json: Optional[str] = None,
    ) -> int:
        with self._connect() as con:
            result = con.execute(
                """
                INSERT INTO rejections
                (url, asset_symbol, reason, source, context, article_id, model, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                """,
                (
                    url,
                    asset_symbol,
                    reason,
                    source,
                    context,
                    article_id,
                    model,
                    details_json,
                    self._to_utc_naive(datetime.now(timezone.utc)),
                ),
            )
            return result.fetchone()[0]