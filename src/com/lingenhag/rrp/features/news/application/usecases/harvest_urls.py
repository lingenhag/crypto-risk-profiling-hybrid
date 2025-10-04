# src/com/lingenhag/rrp/features/news/application/usecases/harvest_urls.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Dict, Optional
import logging
from urllib.parse import urlparse

from com.lingenhag.rrp.domain.models import UrlHarvest, HarvestSummary  # Fixed import
from com.lingenhag.rrp.features.news.application.ports import (
    NewsSourcePort,
    NewsRepositoryPort,
    HarvestCriteriaDTO,
    DomainPolicyPort,
)

_LOG = logging.getLogger(__name__)


def pick_fields(doc: Dict, asset_symbol: str) -> UrlHarvest:
    url = doc.get("og_url") or doc.get("url") or doc.get("link")
    title = doc.get("title") or doc.get("name")
    source = doc.get("source") or doc.get("source_name")
    published = doc.get("published_at") or doc.get("pub_date") or doc.get("seen_at")

    published_at = None
    if isinstance(published, datetime):
        published_at = published.astimezone(timezone.utc)
    elif isinstance(published, str) and published.strip():
        try:
            s = published
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            published_at = datetime.fromisoformat(s)
            if published_at.tzinfo is None:
                published_at = published_at.replace(tzinfo=timezone.utc)
            else:
                published_at = published_at.astimezone(timezone.utc)
        except ValueError:
            _LOG.warning("Invalid published_at format: %s", published)

    return UrlHarvest(
        url=url,
        asset_symbol=asset_symbol,
        source=source,
        published_at=published_at,
        title=title,
        discovered_at=datetime.now(timezone.utc),
    )


def is_valid_news_url(url: Optional[str]) -> bool:
    if not url or not url.strip():
        return False
    if not url.startswith(("http://", "https://")):
        return False
    # Query-Teil ignorieren, nur Pfad-Endung prüfen
    base = url.lower().split("?", 1)[0]
    invalid_extensions = {".jpg", ".png", ".gif", ".pdf"}
    return not any(base.endswith(ext) for ext in invalid_extensions)


def _hostname(u: str) -> Optional[str]:
    try:
        host = (urlparse(u).hostname or "").lower()
        return host or None
    except Exception:
        return None


class HarvestUrls:
    def __init__(
            self,
            sources: List[NewsSourcePort],
            repo: NewsRepositoryPort,
            max_workers: int = 4,
            *,
            domain_policy: Optional[DomainPolicyPort] = None,
            enforce_domain_filter: bool = False,
    ):
        self.sources = sources
        self.repo = repo
        self.max_workers = int(max_workers)
        self.domain_policy = domain_policy
        self.enforce_domain_filter = bool(enforce_domain_filter)
        if self.max_workers < 1:
            _LOG.warning("max_workers < 1, set to 1")
            self.max_workers = 1
        _LOG.debug(
            "HarvestUrls: max_workers type=%s, value=%s",
            type(self.max_workers),
            self.max_workers,
        )

    def run(
            self, criteria: HarvestCriteriaDTO, verbose: bool = False, progress_every: int = 25
    ) -> HarvestSummary:
        """
        Führt den Harvest-Lauf aus und liefert eine zusammengefasste Statistik.

        Counter-Semantik (fixiert durch Tests):
        - total_docs: Anzahl Roh-Dokumente aus allen Quellen (vor jeder Validierung).
        - after_assemble: Anzahl Dokumente, die URL-Validierung und (falls aktiviert)
          den Domain-Filter bestanden haben.
        - after_dedupe: Anzahl Dokumente, die die Dedupe/Persist-Stufe **betreten** haben
          (== after_assemble). Einträge zählen hier auch dann, wenn sie später als
          Duplikat erkannt werden.
        - saved: Anzahl neu persistierter Datensätze (nicht Duplikat).
        - skipped_duplicates: Anzahl erkannter Duplikate (bereits vorhanden).
        - rejected_invalid: Summe aus ungültiger URL **oder** durch Policy geblockter
          Domain **oder** Persistenzfehlern.
        """
        total_docs = 0
        after_assemble = 0
        after_dedupe = 0
        saved = 0
        skipped_duplicates = 0
        rejected_invalid = 0

        processed = 0  # nur für Progress-Logging

        for source in self.sources:
            try:
                docs = source.fetch_documents(criteria)
                total_docs += len(docs)
                _LOG.debug(
                    "Fetched %d documents from source %s",
                    len(docs),
                    source.SOURCE_NAME,
                )
            except Exception:
                _LOG.exception(
                    "fetch_documents failed for source=%s",
                    getattr(source, "SOURCE_NAME", "unknown"),
                )
                continue

            for doc in docs:
                processed += 1
                try:
                    harvest = pick_fields(doc, criteria.asset_symbol)
                    host = _hostname(harvest.url or "")

                    # Grundvalidierung URL
                    if not is_valid_news_url(harvest.url):
                        rejected_invalid += 1
                        if self.domain_policy and host:
                            self.domain_policy.record_harvest(
                                asset_symbol=harvest.asset_symbol,
                                domain=host,
                                stored=False,
                            )
                        continue

                    # Domain-Filter (optional erzwingen)
                    if self.domain_policy and host:
                        allowed = self.domain_policy.is_allowed(
                            harvest.asset_symbol, host
                        )
                        if self.enforce_domain_filter and allowed is False:
                            self.domain_policy.record_harvest(
                                asset_symbol=harvest.asset_symbol,
                                domain=host,
                                stored=False,
                            )
                            rejected_invalid += 1
                            continue

                    # assembled
                    after_assemble += 1

                    try:
                        _, is_duplicate = self.repo.save_url_harvest(
                            url=harvest.url,
                            asset_symbol=harvest.asset_symbol,
                            source=harvest.source,
                            published_at=harvest.published_at,
                            title=harvest.title,
                        )

                        # after_dedupe zählt Eintritt in Dedupe/Persist-Stufe (auch Duplikate)
                        after_dedupe += 1

                        stored_now = not is_duplicate
                        if is_duplicate:
                            skipped_duplicates += 1
                        else:
                            saved += 1

                        if self.domain_policy and host:
                            self.domain_policy.record_harvest(
                                asset_symbol=harvest.asset_symbol,
                                domain=host,
                                stored=stored_now,
                            )
                    except Exception:
                        _LOG.exception("Failed to save URL %s", harvest.url)
                        rejected_invalid += 1
                        if self.domain_policy and host:
                            self.domain_policy.record_harvest(
                                asset_symbol=harvest.asset_symbol,
                                domain=host,
                                stored=False,
                            )

                except Exception:
                    _LOG.exception(
                        "Failed to assemble document from source=%s",
                        getattr(source, "SOURCE_NAME", "unknown"),
                    )
                    rejected_invalid += 1

                if verbose and progress_every > 0 and processed % progress_every == 0:
                    _LOG.info(
                        "Processed %d/%d documents (source=%s)",
                        processed,
                        total_docs,
                        source.SOURCE_NAME,
                    )

        if verbose and (progress_every <= 0 or processed % progress_every != 0):
            _LOG.info("Processed %d documents (batch complete)", processed)

        return HarvestSummary(
            total_docs=total_docs,
            after_assemble=after_assemble,
            after_dedupe=after_dedupe,
            saved=saved,
            skipped_duplicates=skipped_duplicates,
            rejected_invalid=rejected_invalid,
        )

    @staticmethod
    def storage_name(source: NewsSourcePort) -> str:
        return getattr(source, "storage_name", "unknown") or "unknown"