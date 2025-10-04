# src/com/lingenhag/rrp/features/news/infrastructure/sources/gdelt_source.py
from __future__ import annotations

from dataclasses import dataclass

from com.lingenhag.rrp.features.news.infrastructure.sources.base_source import BaseNewsSource
from com.lingenhag.rrp.features.news.infrastructure.gdelt_client import GdeltClient


@dataclass(frozen=True)
class GdeltNewsSource(BaseNewsSource):
    client: GdeltClient
    SOURCE_NAME: str = "gdelt_news"