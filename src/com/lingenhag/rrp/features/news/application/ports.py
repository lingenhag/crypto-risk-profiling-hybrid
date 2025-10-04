# src/com/lingenhag/rrp/features/news/application/ports.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Protocol, Sequence, Dict


@dataclass(frozen=True)
class HarvestCriteriaDTO:
    """
    Kapselt die für einen Harvest-Lauf relevanten Parameter.
    """
    asset_symbol: str
    start: datetime
    end: datetime
    limit: int = 100


# Hinweis: In der Praxis liefern die Infrastruktur-Adapter rohe Dicts
# (mit Feldern wie url, title, published_at, source, raw ...), weil
# externe Quellen unterschiedliche Felder (og_url/link/etc.) mitbringen.
# Der frühere DocumentDTO ist für diese Vielfalt zu starr – wir lassen
# ihn bewusst als Marker für spätere Typisierung bestehen, nutzen aber
# im Port Sequenzen von Dicts.
@dataclass(frozen=True)
class DocumentDTO:  # optional/legacy
    url: str
    title: Optional[str]
    published_at: Optional[datetime]
    source: str


class NewsSourcePort(Protocol):
    """
    Abstrakte Quelle für News-Dokumente (z. B. GDELT, Google RSS).
    Adapter im Infrastruktur-Layer implementieren dieses Protokoll und
    liefern normalisierte Dicts mit mindestens: url, title, source, published_at.
    """
    SOURCE_NAME: str

    def fetch_documents(self, criteria: HarvestCriteriaDTO) -> Sequence[Dict[str, Any]]:
        ...


class UrlResolverPort(Protocol):
    """
    Optionaler Port zum Auflösen/Entschärfen von Redirect-/Consent-URLs
    (z. B. Google News → Publisher-URL).
    """
    def resolve(self, url: str) -> Optional[str]:
        ...


class NewsRepositoryPort(Protocol):
    """
    Persistenz-Schnittstelle für News-Daten (z. B. url_harvests, rejections).
    """
    def save_url_harvest(
            self,
            *,
            url: str,
            asset_symbol: str,
            source: Optional[str],
            published_at: Optional[datetime],
            title: Optional[str],
    ) -> tuple[int, bool]:
        ...

    def save_rejection(
            self,
            *,
            url: str,
            asset_symbol: str,
            reason: str,
            source: Optional[str],
            context: str,
    ) -> int:
        ...

    def now_utc(self) -> datetime:
        ...


# -------- Domain-Policy/Statistik-Ports (Noise-Reduktion) --------
class DomainPolicyPort(Protocol):
    """
    Konfigurierbare Domain-Policy (Allow-/Blocklist) + getrennte Statistiken.
    WICHTIG: 'rejected' ist LLM-spezifisch (Irrelevanz), NICHT Harvest-Fehler.
    """

    def is_allowed(self, asset_symbol: str, domain: str) -> Optional[bool]:
        """
        True/False wenn explizite Policy existiert, sonst None (keine Policy).
        Hinweis: Unsere DuckDB-Repo-Implementierung ist "fail-open" und liefert True,
        daher ist None in der Praxis selten – die Signatur bleibt optional für Future-Proofing.
        """
        ...

    def set_policy(self, *, asset_symbol: str, domain: str, allowed: bool) -> None:
        ...

    def record_harvest(self, *, asset_symbol: str, domain: str, stored: bool) -> None:
        """
        Harvest-Statistik:
        - harvested_total wird IMMER +1 inkrementiert
        - stored_total wird +1 inkrementiert, wenn der Datensatz in url_harvests gespeichert wurde.
        """
        ...

    def record_llm_decision(self, *, asset_symbol: str, domain: str, relevant: bool) -> None:
        """
        LLM-Statistik:
        - llm_accepted wird +1, wenn relevant=True
        - llm_rejected wird +1, wenn relevant=False (kein Asset-Kontext).
        """
        ...


__all__ = [
    "HarvestCriteriaDTO",
    "DocumentDTO",
    "NewsSourcePort",
    "UrlResolverPort",
    "NewsRepositoryPort",
    "DomainPolicyPort",
]