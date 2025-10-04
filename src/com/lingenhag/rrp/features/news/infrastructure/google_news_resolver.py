# src/com/lingenhag/rrp/features/news/infrastructure/google_news_resolver.py
from __future__ import annotations

import logging
import time
from typing import Callable, Optional
from urllib.parse import parse_qs, urlencode, unquote, urlparse

import requests

from com.lingenhag.rrp.features.news.application.ports import UrlResolverPort
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics

_LOG = logging.getLogger(__name__)

_CONSENT_HOSTS = {
    "consent.google.com",
    "consent.yahoo.com",
}

_NEWS_HOST = "news.google.com"
_RESOLVER_NAME = "google_news_resolver"


def _hostname(u: str) -> str:
    return (urlparse(u).hostname or "").lower()


def _is_consent(u: str) -> bool:
    return _hostname(u) in _CONSENT_HOSTS


def _is_news(u: str) -> bool:
    return _hostname(u) == _NEWS_HOST


def _is_google_interstitial(u: str) -> bool:
    parsed = urlparse(u)
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if not host.endswith("google.com"):
        return False
    if "/sorry" in path:
        return True
    qs = parse_qs(parsed.query)
    cont = qs.get("continue", [None])[0]
    if cont and _is_news(unquote(cont)):
        return True
    return False


def _append_us_params(u: str) -> str:
    sep = "&" if "?" in u else "?"
    return f"{u}{sep}{urlencode({'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'})}"


class GoogleNewsResolver(UrlResolverPort):
    """
    Resolver für Google-News-RSS-Artikel-URLs → Publisher-URL.

    Metriken:
      - news_resolver_total{resolver="google_news_resolver", asset="-|<sym>", outcome=...}
      - news_resolver_duration_seconds{resolver="google_news_resolver"}

    Hinweis: Der Resolver kennt das Asset i. d. R. nicht. Wir labeln es deshalb mit "-" ;
    der aufrufende Client (z. B. GoogleNewsRssClient) kann eigene, asset-spezifische
    Source-Metriken reporten.
    """

    def __init__(
            self,
            timeout: int = 20,
            headless_resolve: Optional[Callable[[str], Optional[str]]] = None,
            *,
            resolve_to_publisher: bool = True,
            http_get: Optional[Callable[[str, int, dict[str, str]], object]] = None,
            metrics: Optional[Metrics] = None,
    ) -> None:
        self.timeout = int(timeout)
        self._headless_resolve = headless_resolve
        self._resolve_to_publisher = bool(resolve_to_publisher)
        self._http_get = http_get or self._default_http_get
        self._metrics = metrics

    def resolve(self, url: str) -> Optional[str]:
        if not url:
            return None

        t0 = time.time()
        outcome = "unknown"
        try:
            u = url

            # 1) consent.* → continue=
            if _is_consent(u):
                qs = parse_qs(urlparse(u).query)
                cont = qs.get("continue", [None])[0]
                if not cont:
                    _LOG.debug("GoogleNewsResolver: no 'continue=' param on consent url")
                    outcome = "consent_missing_continue"
                    return None
                u = unquote(cont)

                if self._headless_resolve is not None or not self._resolve_to_publisher:
                    outcome = "returned_news_url"
                    return u
                # else: fall-through to news.google.com

            # 2) news.google.com → bis Publisher auflösen
            if _is_news(u):
                res = self._resolve_news_to_publisher(u)
                outcome = "resolved_publisher" if res and not _is_news(res) else "fallback_news"
                return res

            # 3) andere Hosts → wenn nicht Consent/Interstitial, direkt zurückgeben
            if not _is_consent(u) and not _is_google_interstitial(u):
                outcome = "passthrough"
                return u

            # Interstitial → Headless-Fallback
            if self._is_headless_available():
                res = self._resolve_headless(_append_us_params(u))
                outcome = "headless_resolved" if res else "headless_failed"
                return res
            outcome = "headless_unavailable"
            return None
        except Exception as e:  # noqa: BLE001
            _LOG.warning("GoogleNewsResolver: error resolving %s: %s", url, e)
            outcome = "error"
            return None
        finally:
            if self._metrics:
                self._metrics.track_news_resolver(resolver=_RESOLVER_NAME, asset="-", outcome=outcome)
                self._metrics.track_news_resolver_duration(
                    resolver=_RESOLVER_NAME, duration=max(0.0, time.time() - t0)
                )

    def _resolve_news_to_publisher(self, news_url: str) -> Optional[str]:
        u2 = _append_us_params(news_url)
        try:
            r = self._http_get(
                u2,
                self.timeout,
                {"Referer": "https://news.google.com/", "User-Agent": "Mozilla/5.0"},
            )
            final = getattr(r, "url", None) or u2
            _LOG.debug("GoogleNewsResolver: GET %s → final=%s", u2, final)
        except Exception as e:  # noqa: BLE001
            _LOG.warning("GoogleNewsResolver: error resolving %s: %s", u2, e)
            return news_url

        if (not _is_news(final)) and (not _is_consent(final)) and (not _is_google_interstitial(final)):
            return final

        if self._is_headless_available():
            return self._resolve_headless(u2)

        return news_url

    def _is_headless_available(self) -> bool:
        return self._headless_resolve is not None or _playwright_available()

    def _resolve_headless(self, url_with_params: str) -> Optional[str]:
        if self._headless_resolve:
            try:
                return self._headless_resolve(url_with_params)
            except Exception as e:  # noqa: BLE001
                _LOG.debug("GoogleNewsResolver: injected headless adapter failed: %s", e)
                return None

        try:
            return _playwright_resolve(url_with_params)
        except ImportError:
            _LOG.info("GoogleNewsResolver: playwright not installed; skip headless resolution.")
            return None
        except Exception as e:  # noqa: BLE001
            _LOG.warning("GoogleNewsResolver: headless resolution error: %s", e)
            return None

    @staticmethod
    def _default_http_get(url: str, timeout: int, headers: dict[str, str]) -> object:
        return requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)


def _playwright_available() -> bool:
    try:
        import playwright  # type: ignore[unused-ignore,unused-import]
        return True
    except Exception:
        return False


def _playwright_resolve(url: str) -> Optional[str]:
    from playwright.sync_api import sync_playwright
    from urllib.parse import parse_qs, unquote, urlparse

    def _click_if_present(page, selectors: list[str]) -> bool:
        for sel in selectors:
            try:
                btn = page.locator(sel)
                if btn and btn.first and btn.first.count() >= 0:
                    btn.first.click(timeout=3000)
                    return True
            except Exception:
                continue
        return False

    def _is_news(u: str) -> bool:
        return (urlparse(u).hostname or "").lower() == _NEWS_HOST

    def _is_consent(u: str) -> bool:
        return (urlparse(u).hostname or "").lower() in _CONSENT_HOSTS

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            ctx = browser.new_context()
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            if "consent.google.com" in (page.url or ""):
                _click_if_present(
                    page,
                    [
                        'button:has-text("Accept all")',
                        'button:has-text("I agree")',
                        'text="I agree"',
                        'text="Accept all"',
                        'button:has-text(" Agree")',
                    ],
                )
                page.wait_for_timeout(1000)
                qs = parse_qs(urlparse(page.url).query)
                cont = qs.get("continue", [None])[0]
                if cont:
                    target = unquote(cont)
                    page.goto(target, wait_until="domcontentloaded", timeout=30000)

            if _is_news(page.url):
                page.wait_for_timeout(1500)

            final = page.url
            if final and (not _is_news(final)) and (not _is_consent(final)):
                return final
            return None
        finally:
            browser.close()