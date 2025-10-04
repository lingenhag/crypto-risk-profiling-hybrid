# src/com/lingenhag/rrp/features/news/presentation/cli_commands.py
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
import duckdb
import logging
from pathlib import Path

from com.lingenhag.rrp.platform.config.settings import Settings
from com.lingenhag.rrp.features.news.application.factories import NewsSourceFactory
from com.lingenhag.rrp.features.news.application.usecases.harvest_urls import HarvestUrls
from com.lingenhag.rrp.features.news.application.ports import HarvestCriteriaDTO
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_news_repository import DuckDBNewsRepository
from com.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_domain_policy_repository import DuckDBDomainPolicyRepository
from com.lingenhag.rrp.features.news.infrastructure.repositories.domain_policy_adapter import DomainPolicyAdapter
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.platform.persistence.migrator import apply_migrations

_LOG = logging.getLogger(__name__)


def add_news_subparser(root_subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """
    CLI-Integration für den News-Slice.
    Subcommand: `harvest` (URLs einsammeln → url_harvests, Domain-Policy berücksichtigen)
    """
    news_parser = root_subparsers.add_parser("news", help="News-Slice (Harvest, Inbox, Audit)")
    news_sub = news_parser.add_subparsers(dest="news_cmd", required=True)

    # harvest
    p_harvest = news_sub.add_parser("harvest", help="URLs aus Quellen einsammeln (url_harvests)")
    p_harvest.add_argument("--asset", required=True, help="Asset-Symbol (z. B. DOT, BTC)")
    p_harvest.add_argument("--days", type=int, default=1, help="Zeitraum rückwärts in Tagen (Default: 1)")
    p_harvest.add_argument("--from", dest="date_from", help="Start (ISO, z. B. 2024-01-01T00:00:00Z)")
    p_harvest.add_argument("--to", dest="date_to", help="Ende  (ISO, z. B. 2024-01-02T00:00:00Z)")
    p_harvest.add_argument("--source", choices=["all", "gdelt", "rss"], default="all", help="Quellen")
    p_harvest.add_argument("--limit", type=int, default=100, help="Max. URLs nach Dedupe")
    p_harvest.add_argument(
        "--db",
        help="Pfad zu DuckDB (Default aus config.yaml: database.default_path oder 'data/rrp.duckdb')",
    )
    p_harvest.add_argument("--rss-workers", type=int, default=4, help="Threads für RSS-Auflösung")
    p_harvest.add_argument(
        "--auto-migrate",
        action="store_true",
        help="Falls Schema fehlt: Migrationen automatisch anwenden (platform/persistence/migrations)",
    )
    p_harvest.add_argument(
        "--verbose",
        action="store_true",
        help="Detail-Logging/Progress-Ausgabe aktivieren",
    )
    p_harvest.add_argument(
        "--enforce-domain-filter",
        action="store_true",
        help="Domain-Filter erzwingen (überschreibt config news_domain_filter.enforce)",
    )
    p_harvest.set_defaults(func=_cmd_news_harvest)


def _parse_iso(value: str) -> datetime:
    v = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_time_range(days: int, date_from: Optional[str], date_to: Optional[str]) -> tuple[datetime, datetime]:
    if date_from and date_to:
        start = _parse_iso(date_from)
        end = _parse_iso(date_to)
        if start >= end:
            raise SystemExit("--from muss vor --to liegen.")
        return start, end
    if date_from or date_to:
        raise SystemExit("Bitte beide angeben (--from UND --to) oder keins (dann wird --days verwendet).")
    if days < 1:
        raise SystemExit("--days muss positiv sein.")
    now = datetime.now(timezone.utc)
    return now - timedelta(days=days), now


def _ensure_schema(db_path: str, auto_migrate: bool) -> None:
    """
    Minimal-Check: Wenn `assets` fehlt, laufen die News-Migrationen (001..005) einmalig durch.
    Hinweis: Market-Migrationen (003, 006, 007) sind für Harvest nicht notwendig.
    """
    if not auto_migrate:
        _LOG.info("Skipping schema check as --auto-migrate is not set")
        return
    try:
        repo = DuckDBNewsRepository(db_path=db_path)
        with repo._connect() as con:
            tables = con.execute("SHOW TABLES").fetchall()
            table_names = {row[0] for row in tables}
            if "assets" not in table_names:
                migrations_path = Path("src/com/lingenhag/rrp/platform/persistence/migrations")
                applied = apply_migrations(db_path, str(migrations_path))
                if applied:
                    _LOG.info("[migrate] Applied: %s", ", ".join(applied))
                else:
                    raise RuntimeError("Schema initialization failed")
            else:
                _LOG.info("Schema OK")
    except duckdb.IOException as e:
        raise SystemExit(f"Database error: {e}") from e
    except Exception as e:
        # Falls der erste Check fehlschlägt, versuche es proaktiv mit den Migrationen.
        _LOG.warning("Schema check failed (%s), trying migrations", e)
        migrations_path = Path("src/com/lingenhag/rrp/platform/persistence/migrations")
        applied = apply_migrations(db_path, str(migrations_path))
        if applied:
            _LOG.info("[migrate] Applied: %s", ", ".join(applied))
        else:
            raise SystemExit("Schema initialization failed")


def _cmd_news_harvest(args: argparse.Namespace, *, config: Settings, metrics: Metrics) -> None:
    """
    Führt einen Harvest-Lauf aus:
      - optional: Schema auto-migrieren
      - Quellen instanziieren (gemäß config + CLI)
      - Domain-Filter (Allow-/Blocklist) berücksichtigen
      - Ergebnisse in url_harvests persistieren, Rejections protokollieren
    """
    _ensure_schema(args.db, args.auto_migrate)

    asset = args.asset.upper()
    start, end = _build_time_range(args.days, args.date_from, args.date_to)
    criteria = HarvestCriteriaDTO(asset_symbol=asset, start=start, end=end, limit=args.limit)

    repo = DuckDBNewsRepository(db_path=args.db)
    domain_repo = DuckDBDomainPolicyRepository(db_path=args.db)
    domain_policy = DomainPolicyAdapter(domain_repo)

    # Config-Default lesen; via CLI-Flag kann man es erzwingen
    enforce_cfg = bool(config.get("news_domain_filter", "enforce", False))
    enforce = bool(getattr(args, "enforce_domain_filter", False) or enforce_cfg)

    sources = NewsSourceFactory(config, metrics).create_sources(args.source, args.rss_workers)
    svc = HarvestUrls(
        sources=sources,
        repo=repo,
        max_workers=int(config.get("url_harvest", "max_workers", 4)),
        domain_policy=domain_policy,
        enforce_domain_filter=enforce,
    )

    start_time = time.time()
    summary = svc.run(criteria=criteria, verbose=bool(getattr(args, "verbose", False)), progress_every=25)
    metrics.track_harvest_duration(asset, time.time() - start_time)
    print(
        f"[news/harvest] asset={asset} "
        f"total={summary.total_docs} assembled={summary.after_assemble} "
        f"deduped={summary.after_dedupe} saved={summary.saved} "
        f"duplicates={summary.skipped_duplicates} rejected={summary.rejected_invalid}"
    )