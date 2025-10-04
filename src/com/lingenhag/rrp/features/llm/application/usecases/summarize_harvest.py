# src/com/lingenhag/rrp/features/llm/application/usecases/summarize_harvest.py
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import random  # Jitter

from com.lingenhag.rrp.domain.models import SummarizedArticle
from com.lingenhag.rrp.features.llm.application.ports import LlmPort, VotesRepositoryPort
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_news_repository import DuckDBNewsRepository
from com.lingenhag.rrp.features.news.application.ports import DomainPolicyPort


@dataclass(frozen=True)
class ProcessResult:
    processed: int
    saved: int
    deleted_from_harvest: int
    errors: int
    rejected_irrelevant: int = 0


class _RateLimiter:
    """
    Token-ähnlicher Limiter mit minimalem Jitter zur Thundering-Herd-Vermeidung.
    """
    def __init__(self, calls_per_minute: int) -> None:
        self.interval = 60.0 / max(1, calls_per_minute)
        self._lock = threading.Lock()
        self._next_time = time.monotonic()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                time.sleep(self._next_time - now)
                now = time.monotonic()
            jitter = self.interval * random.uniform(-0.05, 0.05)
            self._next_time = now + self.interval + jitter


class SummarizeHarvest:
    def __init__(
            self,
            llm: LlmPort,
            news_repo: DuckDBNewsRepository,
            votes_repo: VotesRepositoryPort,
            domain_policy: Optional[DomainPolicyPort] = None,
    ) -> None:
        self.llm = llm
        self.news_repo = news_repo
        self.votes_repo = votes_repo
        self.domain_policy = domain_policy

    # -------- helpers --------
    @staticmethod
    def _to_bool_strict(val: Any) -> Optional[bool]:
        """
        Robuste Normalisierung für Booleans aus LLM-Antworten:
        - True/False → unverändert
        - Strings: 'true'/'1'/'yes'/'y'/'ja' → True; 'false'/'0'/'no'/'n'/'nein' → False
        - Zahlen: 0 → False, sonst True
        - Sonst: None (unbestimmt)
        """
        if val is None:
            return None
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            t = val.strip().lower()
            if t in {"true", "1", "yes", "y", "ja"}:
                return True
            if t in {"false", "0", "no", "n", "nein"}:
                return False
            return None
        return None

    def process_batch(
            self,
            *,
            asset_symbol: str,
            limit: int = 10,
            since_utc: Optional[datetime] = None,
            progress_every: int = 25,
            dry_run: bool = False,
    ) -> ProcessResult:
        batch = self.news_repo.fetch_url_harvest_batch(
            asset_symbol=asset_symbol, limit=limit, since_utc=since_utc
        )
        processed = saved = deleted = errors = rejected_irrelevant = 0

        if not batch:
            print(f"[process-harvest] nothing to process for {asset_symbol}.")
            return ProcessResult(0, 0, 0, 0, 0)

        for h in batch:
            processed += 1
            try:
                url: str = h["url"]
                title: Optional[str] = h.get("title")
                h_published = self.news_repo.parse_datetime(h.get("published_at"))
                harvest_id = int(h.get("id", 0)) if isinstance(h.get("id"), (str, int)) else 0

                ai, _, _ = self.llm.summarize_and_score(
                    asset_symbol=asset_symbol,
                    url=url,
                    published_at=h_published.isoformat() if h_published else None,
                    title=title or "",
                )

                # WICHTIG: Nur bei True speichern; None/False → ablehnen.
                ai_relevance = self._to_bool_strict(ai.get("relevance"))
                model_name = getattr(self.llm, "model", "unknown")

                article_id: Optional[int] = None
                if ai_relevance is True:
                    art = self._make_article(h_row=h, asset_symbol=asset_symbol, ai=ai, model_name=model_name)
                    if not dry_run:
                        article_id = self.votes_repo.save_summary(art)
                    saved += 1
                    self._record_llm_domain_stat(url, asset_symbol, accepted=True)
                else:
                    if not dry_run:
                        self.votes_repo.save_rejection(
                            url=url,
                            asset_symbol=asset_symbol,
                            reason="no_asset_relation",
                            source=h.get("source"),
                            context="summarize",
                            article_id=None,
                            model="ensemble",
                            details_json=self._compact_votes_json(ai.get("votes")),
                        )
                    rejected_irrelevant += 1
                    self._record_llm_domain_stat(url, asset_symbol, accepted=False)

                # Persistiere *nur* Einzel-Votes je Modell (keine Ensemble-Zeile)
                for v in (ai.get("votes") or []):
                    if not dry_run:
                        v_rel = self._to_bool_strict(v.get("relevance")) is True
                        self.votes_repo.save_vote(
                            url=url if article_id is None else None,  # URL nur, wenn kein Artikel gespeichert wurde
                            asset_symbol=asset_symbol,
                            model=str(v.get("model") or "unknown"),
                            relevance=v_rel,
                            sentiment=self._round2_opt(v.get("sentiment")),
                            summary=v.get("summary"),
                            harvest_id=harvest_id,
                            article_id=article_id if ai_relevance is True else None,
                        )

                if not dry_run:
                    self.news_repo.delete_url_harvest(harvest_id)
                deleted += 1

            except Exception as exc:
                errors += 1
                print(f"[process-harvest] ERROR on url_id={h.get('id', 'unknown')}: {exc}")
                continue

            if progress_every > 0 and processed % progress_every == 0:
                print(f"[process-harvest] {processed} URLs processed...")

        if processed % (progress_every or 1) != 0:
            print(f"[process-harvest] {processed} URLs processed (batch complete).")

        return ProcessResult(processed, saved, deleted, errors, rejected_irrelevant)

    def process_batch_parallel(
            self,
            *,
            asset_symbol: str,
            limit: int = 25,
            since_utc: Optional[datetime] = None,
            workers: int = 8,
            rate_limit_per_min: int = 60,
            progress_every: int = 25,
            dry_run: bool = False,
    ) -> ProcessResult:
        batch = self.news_repo.fetch_url_harvest_batch(
            asset_symbol=asset_symbol, limit=limit, since_utc=since_utc
        )
        if not batch:
            print(f"[process-harvest] nothing to process for {asset_symbol}.")
            return ProcessResult(0, 0, 0, 0, 0)

        limiter = _RateLimiter(rate_limit_per_min)

        def _llm_task(h: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
            try:
                url: str = h["url"]
                title: Optional[str] = h.get("title")
                h_published = self.news_repo.parse_datetime(h.get("published_at"))
                limiter.wait()
                ai, _, _ = self.llm.summarize_and_score(
                    asset_symbol=asset_symbol,
                    url=url,
                    published_at=h_published.isoformat() if h_published else None,
                    title=title or "",
                )
                return h, {"ok": True, "ai": ai}
            except Exception as exc:
                return h, {"ok": False, "error": exc}

        processed = saved = deleted = errors = rejected_irrelevant = 0

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = [pool.submit(_llm_task, h) for h in batch]

            for fut in as_completed(futures):
                processed += 1
                h, res = fut.result()
                url = h.get("url")
                harvest_id = int(h.get("id", 0)) if isinstance(h.get("id"), (str, int)) else 0

                if not res.get("ok"):
                    errors += 1
                    print(f"[process-harvest] ERROR on url_id={h.get('id', 'unknown')}: {res.get('error')}")
                    continue

                ai = res["ai"]
                try:
                    ai_relevance = self._to_bool_strict(ai.get("relevance"))
                    model_name = getattr(self.llm, "model", "unknown")

                    article_id: Optional[int] = None
                    if ai_relevance is True:
                        art = self._make_article(h_row=h, asset_symbol=asset_symbol, ai=ai, model_name=model_name)
                        if not dry_run:
                            article_id = self.votes_repo.save_summary(art)
                        saved += 1
                        self._record_llm_domain_stat(url, asset_symbol, accepted=True)
                    else:
                        if not dry_run:
                            self.votes_repo.save_rejection(
                                url=url,
                                asset_symbol=asset_symbol,
                                reason="no_asset_relation",
                                source=h.get("source"),
                                context="summarize",
                                article_id=None,
                                model="ensemble",
                                details_json=self._compact_votes_json(ai.get("votes")),
                            )
                        rejected_irrelevant += 1
                        self._record_llm_domain_stat(url, asset_symbol, accepted=False)

                    for v in (ai.get("votes") or []):
                        if not dry_run:
                            v_rel = self._to_bool_strict(v.get("relevance")) is True
                            self.votes_repo.save_vote(
                                url=url if article_id is None else None,
                                asset_symbol=asset_symbol,
                                model=str(v.get("model") or "unknown"),
                                relevance=v_rel,
                                sentiment=self._round2_opt(v.get("sentiment")),
                                summary=v.get("summary"),
                                harvest_id=harvest_id,
                                article_id=article_id if ai_relevance is True else None,
                            )

                    if not dry_run:
                        self.news_repo.delete_url_harvest(harvest_id)
                    deleted += 1

                except Exception as exc:
                    errors += 1
                    print(f"[process-harvest] ERROR on url_id={h.get('id', 'unknown')}: {exc}")
                    continue

                if progress_every > 0 and processed % progress_every == 0:
                    print(f"[process-harvest] {processed} URLs processed...")

        if processed % (progress_every or 1) != 0:
            print(f"[process-harvest] {processed} URLs processed (batch complete).")

        return ProcessResult(processed, saved, deleted, errors, rejected_irrelevant)

    def _make_article(
            self,
            *,
            h_row: Dict[str, Any],
            asset_symbol: str,
            ai: Dict[str, Any],
            model_name: str,
    ) -> SummarizedArticle:
        h_published = self.news_repo.parse_datetime(h_row.get("published_at"))
        h_discovered = self.news_repo.parse_datetime(h_row.get("discovered_at"))
        final_dt = h_published or h_discovered or datetime.now(timezone.utc)

        try:
            sentiment_val = float(ai.get("sentiment")) if ai.get("sentiment") is not None else None
        except Exception:
            sentiment_val = None

        return SummarizedArticle(
            url=h_row["url"],
            published_at=final_dt,
            summary=(ai.get("summary") or "").strip(),
            asset_symbol=asset_symbol,
            source=h_row.get("source"),
            model=model_name,
            sentiment=self._round2_opt(sentiment_val),
            ingested_at=datetime.now(timezone.utc),
        )

    def _record_llm_domain_stat(self, url: Optional[str], asset_symbol: str, *, accepted: bool) -> None:
        if not self.domain_policy or not url:
            return
        try:
            host = (urlparse(url).hostname or "").lower()
            if host:
                self.domain_policy.record_llm_decision(asset_symbol=asset_symbol, domain=host, relevant=accepted)
        except Exception:
            # Domain-Statistik ist "best effort" – keine Hard-Failure
            pass

    @staticmethod
    def _round2_opt(val: Optional[float]) -> Optional[float]:
        if val is None:
            return None
        try:
            return round(float(val), 2)
        except Exception:
            return None

    @staticmethod
    def _compact_votes_json(votes: Optional[List[Dict[str, Any]]]) -> Optional[str]:
        if not votes:
            return None
        compact: List[Dict[str, Any]] = []
        for v in votes:
            compact.append(
                {
                    "model": v.get("model"),
                    "relevance": SummarizeHarvest._to_bool_strict(v.get("relevance")) is True,
                    "sentiment": SummarizeHarvest._round2_opt(v.get("sentiment")),
                }
            )
        try:
            return json.dumps({"votes": compact}, ensure_ascii=False)
        except Exception:
            return None