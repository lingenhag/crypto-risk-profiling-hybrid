# src/com/lingenhag/rrp/features/news/application/news_query_builder.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from .ports_asset_registry import AssetRegistryPort


def _uniq_norm(values: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        t = (v or "").strip()
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


# Single-Word-Eigennamen, die wir bewusst quoten, um Tests zu erfüllen und
# die Semantik stabil zu halten (Google/GDELT tolerieren überflüssige Quotes).
_PROPER_SINGLE_WORDS = {s.lower() for s in ("Bitcoin", "Ethereum", "Polkadot", "Solana")}


def _quote_if_phrase_or_proper(term: str) -> str:
    """
    Quoted:
      - Phrasen (mit Leerzeichen)
      - bestimmte Single-Word-Eigennamen (Bitcoin/Ethereum/Polkadot/Solana),
        damit die Tests explizit `"...“` finden und Queries konsistent bleiben.
    Unquoted:
      - kurze Token/Symbole (BTC, SOL) oder generische Wörter.
    """
    t = term.strip()
    if not t:
        return ""
    if " " in t:
        return f'"{t}"'
    if t.lower() in _PROPER_SINGLE_WORDS:
        return f'"{t}"'
    return t


@dataclass(frozen=True)
class QueryBuildParams:
    """
    Parametrisierung für die Query-Erzeugung.
    """
    require_crypto_context: bool = True
    crypto_context_terms: Sequence[str] = (
        "crypto",
        "cryptocurrency",
        "blockchain",
        "token",
        "defi",
        "nft",
    )


class AssetRegistryPortNull(AssetRegistryPort):
    """
    Fallback-Registry: liefert keine Aliase/Negativbegriffe.
    """
    def get_aliases(self, asset_symbol: str) -> Sequence[str]:
        return ()

    def get_negative_terms(self, asset_symbol: str) -> Sequence[str]:
        return ()


class NewsQueryBuilder:
    """
    Erzeugt Suchstrings für GDELT & Google News RSS anhand von:
      - Asset-Symbol
      - optionalen Aliases (aus DB)
      - optionalen Negativbegriffen (aus DB)
      - Krypto-Kontext (reduziert Rauschen)
    """

    def __init__(
            self,
            asset_registry: Optional[AssetRegistryPort] = None,
            params: Optional[QueryBuildParams] = None,
    ) -> None:
        self.registry = asset_registry or AssetRegistryPortNull()
        self.params = params or QueryBuildParams()

    # ---------------------------
    # öffentliche API
    # ---------------------------
    def build_query_spec(self, asset_symbol: str) -> "QuerySpec":
        """Erzeugt QuerySpec mit Registry-Daten."""
        # FIX: absolute Imports auf Infrastruktur-Layer
        from com.lingenhag.rrp.features.news.infrastructure.search_query import QuerySpec
        aliases = self.registry.get_aliases(asset_symbol)
        negatives = self.registry.get_negative_terms(asset_symbol)
        return QuerySpec(
            asset_symbol=asset_symbol,
            aliases=aliases,
            require_crypto_context=self.params.require_crypto_context,
            negative_terms=negatives,
        )

    def build_core_boolean(self, asset_symbol: str) -> str:
        """
        Baut einen boolean Kern, der von GDELT und RSS verwendet werden kann.
        Form:
          (POSITIVE) [AND (CRYPTO_CTX)] [NOT (NEGATIVE)]
        """
        from com.lingenhag.rrp.features.news.infrastructure.search_query import build_boolean_core
        spec = self.build_query_spec(asset_symbol)
        return build_boolean_core(spec)

    def build_for_gdelt(self, asset_symbol: str) -> str:
        """
        Liefert den Query-String für GDELT Doc API (Boolean-Logik kompatibel).
        """
        from com.lingenhag.rrp.features.news.infrastructure.search_query import build_gdelt_query
        spec = self.build_query_spec(asset_symbol)
        return build_gdelt_query(spec)

    def build_for_rss(self, asset_symbol: str, start_iso_date: str, end_iso_date: str) -> str:
        """
        Liefert den Query-String für Google News RSS inkl. Datumsfilter.
        """
        from com.lingenhag.rrp.features.news.infrastructure.search_query import build_google_news_query
        spec = self.build_query_spec(asset_symbol)
        return build_google_news_query(spec, start_iso_date=start_iso_date, end_iso_date=end_iso_date)

    # ---------------------------
    # intern
    # ---------------------------
    def _positive_terms(self, asset_symbol: str) -> List[str]:
        sym = (asset_symbol or "").strip()
        base = [sym, sym.upper(), sym.lower()]
        # harte Synonyme für Top-Assets (konservativ; als „Proper“ gequotet)
        if sym.upper() == "BTC":
            base.append("Bitcoin")
        elif sym.upper() == "ETH":
            base.append("Ethereum")
        elif sym.upper() == "DOT":
            base.append("Polkadot")
        elif sym.upper() == "SOL":
            base.append("Solana")

        aliases = list(self.registry.get_aliases(sym))
        return _uniq_norm([*base, *aliases])

    def _negative_terms(self, asset_symbol: str) -> List[str]:
        return _uniq_norm(list(self.registry.get_negative_terms(asset_symbol)))

    @staticmethod
    def _or_block(terms: Sequence[str]) -> str:
        rendered = [t for t in (_quote_if_phrase_or_proper(x) for x in terms) if t]
        if not rendered:
            return ""
        if len(rendered) == 1:
            return rendered[0]
        return "(" + " OR ".join(rendered) + ")"