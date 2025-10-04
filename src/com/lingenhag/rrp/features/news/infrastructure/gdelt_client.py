# src/com/lingenhag/rrp/features/news/infrastructure/gdelt_client.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import requests

from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.news.application.ports import HarvestCriteriaDTO
from com.lingenhag.rrp.features.news.application.news_query_builder import NewsQueryBuilder

_LOG = logging.getLogger(__name__)


def _parse_dt_maybe(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    v = value.strip()
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(v, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _floor_day_utc(ts: datetime) -> datetime:
    ts_utc = ts.astimezone(timezone.utc)
    return datetime(ts_utc.year, ts_utc.month, ts_utc.day, tzinfo=timezone.utc)


def _daily_ranges_utc_full_days(start: datetime, end: datetime) -> List[Tuple[datetime, datetime, datetime]]:
    """
    Liefert Tages-Slices NUR für volle UTC-Tage im Intervall:
      Tage von floor(start) (inkl.) bis floor(end) (exkl.).
    Für jeden Tag wird (query_start, query_end, batch_day_start) zurückgegeben,
    wobei query_start=max(start, day_start), query_end=min(end, next_day_start).
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware (UTC recommended)")
    s_day = _floor_day_utc(start)
    e_day_excl = _floor_day_utc(end)
    if s_day >= e_day_excl:
        return []

    slices: List[Tuple[datetime, datetime, datetime]] = []
    day = s_day
    while day < e_day_excl:
        next_day = day + timedelta(days=1)
        q_start = max(start.astimezone(timezone.utc), day)
        q_end = min(end.astimezone(timezone.utc), next_day)
        if q_start < q_end:
            slices.append((q_start, q_end, day))
        day = next_day
    return slices


class GdeltClient:
    SOURCE_NAME = "gdelt"
    BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
    MAX_ITEMS = 250
    RATE_LIMIT_DELAY = 0.6  # ~1000/min

    def __init__(
            self,
            timeout: int = 30,
            max_retries: int = 3,
            metrics: Optional[Metrics] = None,
            *,
            major_assets_without_context: Optional[Set[str]] = None,
            enforce_context_assets: Optional[Set[str]] = None,
    ) -> None:
        self.timeout = int(timeout)
        self.max_retries = int(max_retries)
        self.metrics = metrics
        self.headers = {
            "User-Agent": "com.lingenhag.rrp/1.0 (+https://example.local) python-requests"
        }
        self.major_assets_without_context = {a.upper() for a in (major_assets_without_context or set())}
        self.enforce_context_assets = {a.upper() for a in (enforce_context_assets or set())}
        self.query_builder = NewsQueryBuilder()  # Inject Registry if needed

    def _should_use_crypto_context(self, asset_symbol: str) -> bool:
        sym = (asset_symbol or "").upper()
        if sym in self.enforce_context_assets:
            return True
        if sym in self.major_assets_without_context:
            return False
        return True  # Default: Kontext aktiv

    def fetch_documents(self, criteria: HarvestCriteriaDTO) -> List[Dict]:
        """
        Tagesbasiertes Fetching (UTC). Für jeden Tages-Batch wird `published_at`
        synthetisch auf den Tagesbeginn 00:00:00Z gesetzt. `criteria.limit` gilt pro Tag.
        """
        results: List[Dict] = []

        now = datetime.now(timezone.utc)
        if criteria.start > now or criteria.end > now:
            _LOG.warning(
                "GDELT: future range not supported (start=%s, end=%s, now=%s)",
                criteria.start, criteria.end, now
            )
            return results

        query = self.query_builder.build_for_gdelt(criteria.asset_symbol)
        _LOG.info("GDELT query: %s", query)

        day_slices = _daily_ranges_utc_full_days(criteria.start, criteria.end)
        per_day_limit = max(1, int(criteria.limit))

        for q_start, q_end, batch_day_start in day_slices:
            seen_urls_day: Set[str] = set()
            params = {
                "query": query,
                "mode": "ArtList",
                "format": "json",
                "maxrecords": str(min(self.MAX_ITEMS, per_day_limit)),
                "startdatetime": q_start.strftime("%Y%m%d%H%M%S"),
                "enddatetime": q_end.strftime("%Y%m%d%H%M%S"),
            }

            _LOG.debug("GDELT request params (day): %s", params)
            _LOG.info("GDELT day slice: %s .. %s (limit per day=%d)", q_start, q_end, per_day_limit)

            time.sleep(self.RATE_LIMIT_DELAY)  # Rate-Limit

            t0 = time.time()
            data = self._request_json(params, q_start, q_end)
            duration = max(0.0, time.time() - t0)

            # NEWS metrics per slice
            if self.metrics:
                outcome = "success" if (data and data.get("articles")) else ("no_data" if data else "error")
                self.metrics.track_news_source_fetch(
                    source=self.SOURCE_NAME,
                    asset=criteria.asset_symbol.upper(),
                    outcome=outcome,
                )
                self.metrics.track_news_source_duration(source=self.SOURCE_NAME, duration=duration)

            if data is None:
                _LOG.info("GDELT: no data (error) for day slice %s..%s", q_start, q_end)
                continue

            q_start_iso = q_start.astimezone(timezone.utc).isoformat()
            q_end_iso = q_end.astimezone(timezone.utc).isoformat()

            day_results = 0
            for item in data.get("articles", []):
                if day_results >= per_day_limit:
                    break

                url = (
                        (item.get("url") or "")
                        or item.get("DocumentIdentifier")
                        or item.get("documentIdentifier")
                        or ""
                ).strip()

                if not url or url in seen_urls_day:
                    continue
                seen_urls_day.add(url)

                title = (item.get("title") or item.get("Title") or "").strip()

                raw = dict(item)
                raw["query"] = query
                raw["query_start"] = q_start_iso
                raw["query_end"] = q_end_iso

                published_at = batch_day_start

                results.append(
                    {
                        "url": url,
                        "title": title,
                        "source": self.SOURCE_NAME,
                        "published_at": published_at,
                        "content": "",
                        "raw": raw,
                    }
                )
                day_results += 1

            _LOG.info("GDELT fetched %d documents for day %s", day_results, batch_day_start.date())

        _LOG.info("GDELT total documents across days: %d", len(results))
        return results

    def _request_json(self, params: Dict, start: datetime, end: datetime) -> Optional[Dict]:
        attempt = 0
        backoff = 1.0
        while attempt < self.max_retries:
            attempt += 1
            t0 = time.time()
            resp: Optional[requests.Response] = None
            try:
                resp = requests.get(
                    self.BASE_URL,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout,
                )
                ct = (resp.headers.get("Content-Type") or "").lower()
                status = resp.status_code

                if status in (429, 500, 502, 503, 504):
                    _LOG.warning(
                        "GDELT: HTTP %s (attempt %s/%s) range=%s..%s; backoff %.1fs",
                        status, attempt, self.max_retries, start, end, backoff
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                resp.raise_for_status()

                if "application/json" not in ct:
                    txt = (resp.text or "")[:200].replace("\n", " ")
                    _LOG.warning(
                        "GDELT: non-JSON response (ct='%s', status=%s) range=%s..%s; snippet='%s'",
                        ct, status, start, end, txt
                    )
                    if self.metrics:
                        self.metrics.track_api_request("gdelt", "error")
                        self.metrics.track_api_duration("gdelt", max(0.0, time.time() - t0))
                    return None

                if self.metrics:
                    self.metrics.track_api_request("gdelt", "success")
                    self.metrics.track_api_duration("gdelt", max(0.0, time.time() - t0))
                return resp.json()

            except requests.RequestException as e:
                if self.metrics:
                    self.metrics.track_api_request("gdelt", "error")
                    self.metrics.track_api_duration("gdelt", max(0.0, time.time() - t0))
                _LOG.warning(
                    "GDELT: request error %s (attempt %s/%s) range=%s..%s",
                    e, attempt, self.max_retries, start, end
                )
                time.sleep(backoff)
                backoff *= 2

            except ValueError as e:
                txt = (resp.text[:200].replace("\n", " ") if resp is not None else "")
                _LOG.warning(
                    "GDELT: JSON parse error %s; snippet='%s' range=%s..%s",
                    e, txt, start, end
                )
                if self.metrics:
                    self.metrics.track_api_request("gdelt", "error")
                    self.metrics.track_api_duration("gdelt", max(0.0, time.time() - t0))
                return None

        _LOG.error("GDELT: all retries failed for range=%s..%s", start, end)
        if self.metrics:
            self.metrics.track_api_request("gdelt", "error")
        return None