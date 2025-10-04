# src/com/lingenhag/rrp/features/news/infrastructure/repositories/domain_policy_adapter.py
from __future__ import annotations

from com.lingenhag.rrp.features.news.application.ports import DomainPolicyPort
from .duckdb_domain_policy_repository import DuckDBDomainPolicyRepository


class DomainPolicyAdapter(DomainPolicyPort):
    """
    Adapter, der das konkrete DuckDB-Repository auf das Domain-Port abbildet.
    Vereinheitlicht Methodennamen und Signaturen.
    """

    def __init__(self, repo: DuckDBDomainPolicyRepository) -> None:
        self._repo = repo

    # ---- DomainPolicyPort ----
    def is_allowed(self, asset_symbol: str, domain: str) -> bool | None:
        # Repository ist fail-open; None (keine Policy) signalisieren wir hier nicht,
        # da is_allowed bereits bool liefert. Für Kompatibilität geben wir bool zurück.
        return self._repo.is_allowed(asset_symbol, domain)

    def set_policy(self, *, asset_symbol: str, domain: str, allowed: bool) -> None:
        self._repo.allow(asset_symbol, domain, allowed=allowed)

    def record_harvest(self, *, asset_symbol: str, domain: str, stored: bool) -> None:
        self._repo.record_harvest(asset_symbol, domain, stored=stored)

    def record_llm_decision(self, *, asset_symbol: str, domain: str, relevant: bool) -> None:
        self._repo.record_llm_decision(asset_symbol, domain, accepted=relevant)