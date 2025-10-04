# src/com/lingenhag/rrp/features/news/infrastructure/search_query.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Set


@dataclass(frozen=True)
class QuerySpec:
    """
    Konfiguration für die Query-Erzeugung von GDELT/Google News.
    """
    asset_symbol: str
    aliases: Sequence[str] = ()
    require_crypto_context: bool = True
    extra_positive_terms: Sequence[str] = ()
    negative_terms: Sequence[str] = ()
    min_token_quote_len: int = 4


def _norm_terms(terms: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for t in terms:
        t = (t or "").strip()
        if not t or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    return out


def _render_term(t: str, *, min_quote_len: int) -> str:
    t = t.strip()
    if not t:
        return ""
    if " " in t:
        return f'"{t}"'
    # kurze Tokens nicht quoten (z. B. BTC, SOL)
    return t


def _symbol_synonyms(symbol: str) -> List[str]:
    sym = (symbol or "").strip()
    if not sym:
        return []
    out = [sym, sym.upper(), sym.lower()]
    if sym.upper() == "BTC":
        out.append("Bitcoin")
    if sym.upper() == "ETH":
        out.append("Ethereum")
    if sym.upper() == "DOT":
        out.append("Polkadot")
    if sym.upper() == "SOL":
        out.append("Solana")
    return _norm_terms(out)


def build_boolean_core(spec: QuerySpec) -> str:
    """
    Baut den kombinierten Boolean-Query-String, der in GDELT/Google News
    wiederverwendet werden kann (ohne Datums-/API-spezifische Ergänzungen).
    Struktur:
      (aliases/symbole) AND (crypto-kontext) [AND extra] [NOT negatives]
    """
    positives = _norm_terms([*_symbol_synonyms(spec.asset_symbol), *spec.aliases, *spec.extra_positive_terms])
    crypto_ctx = ["crypto", "cryptocurrency", "blockchain", "token", "defi", "nft"] if spec.require_crypto_context else []
    negatives = _norm_terms(spec.negative_terms)

    def or_block(terms: Sequence[str]) -> Optional[str]:
        if not terms:
            return None
        rendered = [_render_term(t, min_quote_len=spec.min_token_quote_len) for t in terms]
        rendered = [t for t in rendered if t]
        if not rendered:
            return None
        return "(" + " OR ".join(rendered) + ")" if len(rendered) > 1 else rendered[0]

    must_pos = or_block(positives)
    must_ctx = or_block(crypto_ctx)
    not_block = or_block(negatives)

    parts: List[str] = []
    if must_pos:
        parts.append(must_pos)
    if must_ctx:
        parts.append(must_ctx)
    if spec.extra_positive_terms:
        extra_block = or_block(spec.extra_positive_terms)
        if extra_block:
            parts.append(extra_block)
    if not_block:
        parts.append(f"NOT {not_block}")

    if not parts:
        parts.append(_render_term(spec.asset_symbol, min_quote_len=spec.min_token_quote_len))

    return " AND ".join(parts)


def build_gdelt_query(spec: QuerySpec) -> str:
    return build_boolean_core(spec)


def build_google_news_query(spec: QuerySpec, *, start_iso_date: str, end_iso_date: str) -> str:
    core = build_boolean_core(spec)
    return f"{core} after:{start_iso_date} before:{end_iso_date}"