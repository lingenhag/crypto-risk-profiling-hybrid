# src/com/lingenhag/rrp/features/news/infrastructure/google_news_rss_client.py
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Set
from urllib.parse import urlencode, quote_plus
import xml.etree.ElementTree as ET
import email.utils as eut
import requests

from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.news.application.ports import HarvestCriteriaDTO
from com.lingenhag.rrp.features.news.infrastructure.google_news_resolver import GoogleNewsResolver

_LOG = logging.getLogger(__name__)


def _parse_pubdate(pub_date: Optional[str]) -> Optional[datetime]:
    if not pub_date:
        return None
    try:
        dt = eut.parsedate_to_datetime(pub_date)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _within_range(ts: Optional[datetime], start: datetime, end: datetime) -> bool:
    if ts is None:
        return True
    return start <= ts <= end


def _find_publisher(item: ET.Element) -> Optional[str]:
    # robust gegen Namespaces
    src_el = item.find("source")
    if src_el is None:
        src_el = item.find("{http://www.w3.org/2005/Atom}source")
    if src_el is None:
        for child in list(item):
            tag = child.tag
            if isinstance(tag, str) and tag.rsplit("}", 1)[-1] == "source":
                src_el = child
                break
    if src_el is None:
        return None
    text = (src_el.text or "").strip()
    if text:
        return text
    url_attr = (src_el.get("url") or "").strip()
    return url_attr or None


@dataclass
class GoogleNewsRssClient:
    """
    RSS-Client für Google News.
    """
    hl: str = "en-US"
    gl: str = "US"
    ceid: str = "US:en"
    timeout: int = 30
    resolve_redirects: bool = True
    max_workers: int = 4
    metrics: Optional[Metrics] = None
    http_fetch: Optional[Callable[[str, int], str]] = None  # (url, timeout) -> text
    resolver: Optional[GoogleNewsResolver] = None

    # Neu: konfigurierbare Kontext-Politik
    major_assets_without_context: Optional[Set[str]] = None
    enforce_context_assets: Optional[Set[str]] = None

    SOURCE_NAME: str = "google_rss"  # ← zurück auf den erwarteten Namen
    BASE_URL: str = "https://news.google.com/rss/search"

    def _default_http_fetch(self, url: str, timeout: int) -> str:
        r = requests.get(
            url,
            headers={"User-Agent": "com.lingenhag.rrp/1.0 (+https://example.local) python-requests"},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.text

    def _should_use_crypto_context(self, asset_symbol: str) -> bool:
        major = {a.upper() for a in (self.major_assets_without_context or set())}
        enforce = {a.upper() for a in (self.enforce_context_assets or set())}
        sym = (asset_symbol or "").upper()
        if sym in enforce:
            return True
        if sym in major:
            return False
        return True  # Default: Kontext aktiv

    def _build_query(self, criteria: HarvestCriteriaDTO) -> str:
        """
        Google-News-Query: (SYMBOL OR "Langname") [+ optionaler Kontext] + after:/before:
        """
        use_context = self._should_use_crypto_context(criteria.asset_symbol)
        hard_name = "Bitcoin" if criteria.asset_symbol.upper() == "BTC" else None
        core_terms: List[str] = [criteria.asset_symbol.upper()]
        if hard_name:
            core_terms.append(f'"{hard_name}"')
        core = "(" + " OR ".join(core_terms) + ")"

        if use_context:
            context = " AND (crypto OR cryptocurrency OR blockchain OR token OR defi OR nft)"
        else:
            context = ""

        start_d = criteria.start.date().isoformat()
        end_d = criteria.end.date().isoformat()
        date_clause = f" after:{start_d} before:{end_d}"

        q = core + context + date_clause
        _LOG.info("GoogleNewsRssClient query: %s", q)
        return q

    def _build_url(self, query: str) -> str:
        params = {"q": query, "hl": self.hl, "gl": self.gl, "ceid": self.ceid}
        return f"{self.BASE_URL}?{urlencode(params, quote_via=quote_plus)}"

    def fetch_documents(self, criteria: HarvestCriteriaDTO) -> List[Dict]:
        query = self._build_query(criteria)
        url = self._build_url(query)
        _LOG.info("GoogleNewsRssClient: fetching RSS for query=%s", query)

        fetch = self.http_fetch or self._default_http_fetch
        t0 = time.time()
        source_label = self.SOURCE_NAME
        asset_label = criteria.asset_symbol.upper()

        try:
            xml_text = fetch(url, self.timeout)
            if self.metrics:
                self.metrics.track_news_source_fetch(source=source_label, asset=asset_label, outcome="success")
                self.metrics.track_news_source_duration(
                    source=source_label, duration=max(0.0, time.time() - t0)
                )
                # legacy generic metrics
                self.metrics.track_api_request("google_news_rss", "success")
                self.metrics.track_api_duration("google_news_rss", max(0.0, time.time() - t0))
        except Exception as e:
            _LOG.warning("GoogleNewsRssClient: fetch error for %s: %s", url, e)
            if self.metrics:
                self.metrics.track_news_source_fetch(source=source_label, asset=asset_label, outcome="error")
                self.metrics.track_news_source_duration(
                    source=source_label, duration=max(0.0, time.time() - t0)
                )
                self.metrics.track_api_request("google_news_rss", "error")
                self.metrics.track_api_duration("google_news_rss", max(0.0, time.time() - t0))
            return []

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            _LOG.warning("GoogleNewsRssClient: XML parse error: %s", e)
            if self.metrics:
                self.metrics.track_news_source_fetch(source=source_label, asset=asset_label, outcome="parse_error")
            return []

        items: List[Dict] = []

        # Resolver bereitstellen (mit Metrics)
        resolver = self.resolver or GoogleNewsResolver(timeout=self.timeout, metrics=self.metrics)

        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_raw = item.findtext("pubDate")
            published_at = _parse_pubdate(pub_raw)

            publisher_name = _find_publisher(item)

            if not _within_range(published_at, criteria.start, criteria.end):
                continue

            final_url = link
            if self.resolve_redirects:
                resolved = resolver.resolve(link)
                if resolved:
                    final_url = resolved

            raw = {
                "rss_link": link,
                "query": query,
                "hl": self.hl,
                "gl": self.gl,
                "ceid": self.ceid,
                "pubDate": pub_raw,
                "publisher": publisher_name,
            }

            items.append(
                {
                    "url": final_url,
                    "title": title,
                    "source": self.SOURCE_NAME,
                    "published_at": published_at,
                    "content": "",
                    "raw": raw,
                }
            )
            if len(items) >= max(1, int(criteria.limit)):
                break

        outcome = "no_items" if not items else "assembled"
        if self.metrics:
            self.metrics.track_news_source_fetch(source=source_label, asset=asset_label, outcome=outcome)

        _LOG.info("GoogleNewsRssClient: assembled %d items (limit=%d).", len(items), criteria.limit)
        return items