# src/com/lingenhag/rrp/features/news/infrastructure/sources/base_source.py
from __future__ import annotations

from typing import Any, Dict, Sequence

from com.lingenhag.rrp.features.news.application.ports import HarvestCriteriaDTO, NewsSourcePort


class BaseNewsSource(NewsSourcePort):
    SOURCE_NAME: str = "base"

    def __init__(self, client):
        self.client = client
        self.storage_name = self.SOURCE_NAME

    def fetch_documents(self, criteria: HarvestCriteriaDTO) -> Sequence[Dict[str, Any]]:
        # Direkte Rückgabe von List[Dict] aus dem jeweiligen Client.
        return self.client.fetch_documents(criteria)