# src/com/lingenhag/rrp/features/news/application/factories.py
from __future__ import annotations
from typing import List, Set

from com.lingenhag.rrp.platform.config.settings import Settings
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.news.application.ports import NewsSourcePort
from com.lingenhag.rrp.features.news.infrastructure.gdelt_client import GdeltClient
from com.lingenhag.rrp.features.news.infrastructure.sources.google_rss_source import GoogleRssNewsSource
from com.lingenhag.rrp.features.news.infrastructure.google_news_rss_client import GoogleNewsRssClient

class NewsSourceFactory:
    def __init__(self, config: Settings, metrics: Metrics):
        self.config = config
        self.metrics = metrics

    def _read_context_policy(self) -> tuple[Set[str], Set[str]]:
        majors = set(self.config.get("news_query", "major_assets_without_context", []) or [])
        enforce = set(self.config.get("news_query", "enforce_context_assets", []) or [])
        majors = {s.upper() for s in majors}
        enforce = {s.upper() for s in enforce}
        return majors, enforce

    def create_sources(self, source: str, rss_workers: int) -> List[NewsSourcePort]:
        sources: List[NewsSourcePort] = []
        gdelt_enabled = self.config.get("gdelt", "enabled", True)
        google_news_enabled = self.config.get("google_news", "enabled", True)

        majors, enforce = self._read_context_policy()

        if source == "all":
            if gdelt_enabled:
                sources.append(GdeltClient(
                    timeout=self.config.get("gdelt", "timeout", 30),
                    max_retries=self.config.get("gdelt", "max_retries", 3),
                    metrics=self.metrics,
                    major_assets_without_context=majors,
                    enforce_context_assets=enforce,
                ))
            if google_news_enabled:
                sources.append(GoogleRssNewsSource(
                    client=GoogleNewsRssClient(
                        hl=self.config.get("google_news", "hl", "en-US"),
                        gl=self.config.get("google_news", "gl", "US"),
                        ceid=self.config.get("google_news", "ceid", "US:en"),
                        timeout=self.config.get("google_news", "timeout", 60),
                        resolve_redirects=self.config.get("google_news", "resolve_redirects", True),
                        max_workers=int(rss_workers),
                        metrics=self.metrics,
                        major_assets_without_context=majors,
                        enforce_context_assets=enforce,
                    )
                ))
        elif source == "gdelt" and gdelt_enabled:
            sources.append(GdeltClient(
                timeout=self.config.get("gdelt", "timeout", 30),
                max_retries=self.config.get("gdelt", "max_retries", 3),
                metrics=self.metrics,
                major_assets_without_context=majors,
                enforce_context_assets=enforce,
            ))
        elif source in ("rss", "google_rss") and google_news_enabled:
            sources.append(GoogleRssNewsSource(
                client=GoogleNewsRssClient(
                    hl=self.config.get("google_news", "hl", "en-US"),
                    gl=self.config.get("google_news", "gl", "US"),
                    ceid=self.config.get("google_news", "ceid", "US:en"),
                    timeout=self.config.get("google_news", "timeout", 60),
                    resolve_redirects=self.config.get("google_news", "resolve_redirects", True),
                    max_workers=int(rss_workers),
                    metrics=self.metrics,
                    major_assets_without_context=majors,
                    enforce_context_assets=enforce,
                )
            ))
        else:
            valid = ", ".join(("all", "gdelt", "google_rss", "rss"))
            raise SystemExit(f"Ungültige News-Quelle '{source}'. Erlaubt: {valid}")

        return sources