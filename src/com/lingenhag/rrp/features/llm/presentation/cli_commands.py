# src/com/lingenhag/rrp/features/llm/presentation/cli_commands.py
from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import duckdb

from com.lingenhag.rrp.domain.models import CryptoAsset, TimeRange
from com.lingenhag.rrp.features.llm.application.ports import LlmPort
from com.lingenhag.rrp.features.llm.application.usecases.summarize_harvest import (
    SummarizeHarvest,
)
from com.lingenhag.rrp.features.llm.infrastructure.repositories.duckdb_llm_repository import (
    DuckDBLLMRepository,
)
# Korrekte Adapter-Importe; wir stellen eine Alias-Klasse bereit, falls der alte Name verwendet wurde.
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_domain_policy_repository import (
    DuckDBDomainPolicyRepository,
)
from com.lingenhag.rrp.features.news.infrastructure.repositories.domain_policy_adapter import (
    DomainPolicyAdapter as _DomainPolicyAdapter,
)
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_news_repository import (
    DuckDBNewsRepository,
)
from com.lingenhag.rrp.platform.config.settings import Settings
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics

# Öffentliche Exporte dieses Moduls
__all__ = [
    "add_llm_subparser",
    "_build_llm_from_config",
]


# ---------------------------------------------------------------------------
# CLI Subparser
# ---------------------------------------------------------------------------
def add_llm_subparser(
        root_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Registriert den 'llm' Subparser mit dem 'process' Befehl.
    """
    llm_parser = root_subparsers.add_parser(
        "llm",
        help="LLM processing (summarize and score)",
    )
    llm_sub = llm_parser.add_subparsers(dest="llm_cmd", required=True)

    p_process = llm_sub.add_parser("process", help="Process URLs with LLMs")
    p_process.add_argument("--asset", required=True, help="Asset-Symbol (e.g., BTC, DOT)")
    p_process.add_argument("--days", type=int, default=1, help="Time range in days (default: 1)")
    p_process.add_argument("--from", dest="date_from", help="Start (ISO, e.g., 2024-01-01T00:00:00Z)")
    p_process.add_argument("--to", dest="date_to", help="End (ISO, e.g., 2024-01-02T00:00:00Z)")
    p_process.add_argument("--limit", type=int, default=10, help="Max URLs to process")
    # Wichtig: --db Argument vorhanden, Default wird später in main() ggf. aus config überschrieben.
    p_process.add_argument("--db", default="data/pm.duckdb", help="Path to DuckDB")
    p_process.add_argument("--parallel", action="store_true", help="Run in parallel")
    p_process.add_argument(
        "--workers", type=int, default=8, help="Number of workers for parallel processing"
    )
    p_process.add_argument(
        "--rate-limit", type=int, default=60, help="Rate limit per minute (per model/client)"
    )
    p_process.add_argument(
        "--export-votes-csv",
        dest="export_votes_csv",
        help="Export llm_votes filtered by asset/timewindow to this CSV file",
    )
    # NEW: Dry-Run ohne DB-Schreiboperationen
    p_process.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate processing without writing to DB or deleting harvest entries",
    )
    p_process.set_defaults(func=_cmd_llm_process)


# ---------------------------------------------------------------------------
# Helpers (Parsing & Export)
# ---------------------------------------------------------------------------
def _parse_iso(value: str) -> datetime:
    v = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_time_range(days: int, date_from: Optional[str], date_to: Optional[str]) -> TimeRange:
    if date_from and date_to:
        start = _parse_iso(date_from)
        end = _parse_iso(date_to)
        if start >= end:
            raise SystemExit("--from must be before --to")
        return TimeRange(start=start, end=end)
    if date_from or date_to:
        raise SystemExit("Specify both --from and --to or neither (uses --days)")
    if days < 1:
        raise SystemExit("--days must be positive")
    now = datetime.now(timezone.utc)
    return TimeRange(start=now - timedelta(days=days), end=now)


def _export_votes_csv(
        *,
        db_path: str,
        asset_symbol: str,
        since_utc: datetime,
        out_path: str,
) -> int:
    """
    Exportiert llm_votes für ein Asset ab since_utc in eine CSV-Datei.
    Gibt die Anzahl exportierter Zeilen zurück.
    """
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    query = """
            SELECT
                id,
                url,
                asset_symbol,
                model,
                relevance,
                sentiment,
                summary,
                created_at,
                harvest_id,
                article_id
            FROM llm_votes
            WHERE asset_symbol = ?
              AND created_at >= ?
            ORDER BY created_at ASC, id ASC \
            """

    with duckdb.connect(db_path) as con:
        con.execute("SET TimeZone='UTC'")
        rows = con.execute(query, (asset_symbol, since_utc)).fetchall()
        cols = [d[0] for d in con.description]

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for r in rows:
            writer.writerow(_stringify_row(r))

    return len(rows)


def _stringify_row(row: Iterable[object]) -> list[str]:
    out: list[str] = []
    for v in row:
        if isinstance(v, bool):
            out.append("true" if v else "false")
        elif isinstance(v, (int, float)):
            if isinstance(v, float):
                out.append(f"{v:.2f}")
            else:
                out.append(str(v))
        elif isinstance(v, datetime):
            out.append(v.astimezone(timezone.utc).isoformat())
        elif v is None:
            out.append("")
        else:
            out.append(str(v))
    return out


# ---------------------------------------------------------------------------
# Command Implementation
# ---------------------------------------------------------------------------
def _cmd_llm_process(args: argparse.Namespace, config: Settings, metrics: Metrics, llm: LlmPort) -> None:
    asset_sym = args.asset.upper()
    asset_obj = CryptoAsset(symbol=asset_sym, name=asset_sym, aliases={asset_sym})
    time_range = _build_time_range(args.days, args.date_from, args.date_to)

    # Repositories
    news_repo = DuckDBNewsRepository(db_path=args.db)
    votes_repo = DuckDBLLMRepository(db_path=args.db)

    # Domain-Policy/Stats injizieren (Adapter → Port)
    domain_repo = DuckDBDomainPolicyRepository(db_path=args.db)
    domain_policy = _DomainPolicyAdapter(domain_repo)  # Alias auf DomainPolicyAdapter

    proc = SummarizeHarvest(
        llm=llm,
        news_repo=news_repo,
        votes_repo=votes_repo,
        domain_policy=domain_policy,
    )

    start_time = time.time()
    if args.parallel:
        res = proc.process_batch_parallel(
            asset_symbol=asset_obj.symbol,
            limit=args.limit,
            since_utc=time_range.start,
            workers=args.workers,
            rate_limit_per_min=args.rate_limit,
            dry_run=bool(getattr(args, "dry_run", False)),
        )
        metrics.track_summarize_duration(asset_obj.symbol, "parallel", time.time() - start_time)
        print(
            f"[llm-process-par] asset={asset_obj.symbol} processed={res.processed} "
            f"saved={res.saved} deleted={res.deleted_from_harvest} errors={res.errors} "
            f"rejected={res.rejected_irrelevant} workers={args.workers} rate_limit={args.rate_limit}/min "
            f"dry_run={bool(getattr(args, 'dry_run', False))}"
        )
    else:
        res = proc.process_batch(
            asset_symbol=asset_obj.symbol,
            limit=args.limit,
            since_utc=time_range.start,
            dry_run=bool(getattr(args, "dry_run", False)),
        )
        metrics.track_summarize_duration(asset_obj.symbol, "sequential", time.time() - start_time)
        print(
            f"[llm-process] asset={asset_obj.symbol} processed={res.processed} "
            f"saved={res.saved} deleted={res.deleted_from_harvest} errors={res.errors} "
            f"rejected={res.rejected_irrelevant} dry_run={bool(getattr(args, 'dry_run', False))}"
        )

    # Optionaler CSV-Export der Votes (Auditing)
    if getattr(args, "export_votes_csv", None):
        out_file = str(args.export_votes_csv)
        exported = _export_votes_csv(
            db_path=args.db,
            asset_symbol=asset_obj.symbol,
            since_utc=time_range.start,
            out_path=out_file,
        )
        print(f"[llm-process] votes exported → {out_file} ({exported} rows)")


# ---------------------------------------------------------------------------
# LLM Factory (Lazy import – wird in main() referenziert)
# ---------------------------------------------------------------------------
def _build_llm_from_config(config: Settings, metrics: Metrics) -> LlmPort:
    """
    Baut das LLM (Ensemble oder Single) gemäss Konfiguration.
    Lazy-Imports vermeiden harte Abhängigkeiten beim Laden des CLI-Moduls.
    """
    # 🔧 FIX: Korrekte Pfade – die Clients liegen im *infrastructure*-Package
    from com.lingenhag.rrp.features.llm.infrastructure.ensemble_client import EnsembleClient
    from com.lingenhag.rrp.features.llm.infrastructure.gemini_client import GeminiClient
    from com.lingenhag.rrp.features.llm.infrastructure.openai_client import OpenAIClient
    from com.lingenhag.rrp.features.llm.infrastructure.xai_client import XAIClient

    use_openai = bool(config.get("ensemble", "use_openai", True))
    use_gemini = bool(config.get("ensemble", "use_gemini", True))
    use_xai = bool(config.get("ensemble", "use_xai", True))

    clients = []

    if use_openai:
        clients.append(
            OpenAIClient(
                api_key=None,  # aus ENV lesen
                model=str(config.get("openai", "model", "gpt-5")),
                endpoint=str(config.get("openai", "endpoint", "https://api.openai.com/v1/chat/completions")),
                timeout=int(config.get("openai", "timeout", 60)),
                fallback_model=config.get("openai", "fallback_model", None),
                prompt_file=str(config.get("openai", "prompt_file", "prompts/summarize_sentiment.txt")),
                metrics=metrics,
                max_tokens=int(config.get("openai", "max_tokens", 400)),
                # optionale Felder aus config.yaml
                max_tokens_cap=config.get("openai", "max_tokens_cap", None),
                auto_scale_max_tokens=bool(config.get("openai", "auto_scale_max_tokens", True)),
                temperature=float(config.get("openai", "temperature", 0.0)),
                response_format=str(config.get("openai", "response_format", "json_object")),
            )
        )

    if use_gemini:
        gemini_cap = config.get("gemini", "max_output_tokens_cap", None)
        clients.append(
            GeminiClient(
                api_key=None,  # aus ENV lesen
                model=str(config.get("gemini", "model", "gemini-2.5-flash")),
                endpoint=str(config.get("gemini", "endpoint", "https://generativelanguage.googleapis.com/v1beta/models")),
                timeout=int(config.get("gemini", "timeout", 60)),
                prompt_file=str(config.get("gemini", "prompt_file", "prompts/summarize_sentiment.txt")),
                metrics=metrics,
                max_tokens=int(config.get("gemini", "max_tokens", 400)),
                max_output_tokens_cap=int(gemini_cap) if gemini_cap is not None else None,
                temperature=float(config.get("gemini", "temperature", 0.0)),
                response_mime_type=str(config.get("gemini", "response_mime_type", "application/json")),
            )
        )

    if use_xai:
        clients.append(
            XAIClient(
                api_key=None,  # aus ENV lesen
                model=str(config.get("xai", "model", "grok-4")),
                endpoint=str(config.get("xai", "endpoint", "https://api.x.ai/v1/chat/completions")),
                timeout=int(config.get("xai", "timeout", 60)),
                prompt_file=str(config.get("xai", "prompt_file", "prompts/summarize_sentiment.txt")),
                metrics=metrics,
                max_tokens=int(config.get("xai", "max_tokens", 1200)),
                # optionale Felder aus config.yaml
                max_tokens_cap=config.get("xai", "max_tokens_cap", None),
                auto_scale_max_tokens=bool(config.get("xai", "auto_scale_max_tokens", True)),
                temperature=float(config.get("xai", "temperature", 0.0)),
            )
        )

    return EnsembleClient(clients=clients)