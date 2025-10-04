# src/com/lingenhag/rrp/features/news/infrastructure/sources/google_rss_source.py
from __future__ import annotations
from dataclasses import dataclass
from com.lingenhag.rrp.features.news.infrastructure.sources.base_source import BaseNewsSource
from com.lingenhag.rrp.features.news.infrastructure.google_news_rss_client import GoogleNewsRssClient

@dataclass(frozen=True)
class GoogleRssNewsSource(BaseNewsSource):
    client: GoogleNewsRssClient
    SOURCE_NAME: str = "google_rss"