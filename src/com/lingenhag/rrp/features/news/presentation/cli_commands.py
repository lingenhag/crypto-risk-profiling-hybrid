com.lingenhag.rrpfeatures/news/presentation/cli_commands.py
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
import duckdb
import logging
from pathlib import Path

from ccom.lingenhag.rrp.platform.config.settings import Settings
from ccom.lingenhag.rrp.features.news.application.factories import NewsSourceFactory
from ccom.lingenhag.rrp.features.news.application.usecases.harvest_urls import HarvestUrls
from ccom.lingenhag.rrp.features.news.application.ports import HarvestCriteriaDTO
from ccom.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_news_repository import DuckDBNewsRepository
from ccom.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_domain_policy_repository import DuckDBDomainPolicyRepository
from ccom.lingenhag.rrp.features.news.infrastructure.repositories.domain_policy_adapter import DomainPolicyAdapter
from ccom.lingenhag.rrp.features.news.infrastructure.repositories.duckdb_asset_registry import DuckDBAssetRegistryRepository
from ccom.lingenhag.rrp.features.news.application.news_query_builder import NewsQueryBuilder
from ccom.lingenhag.rrp.platform.monitoring.metrics import Metrics
from ccom.lingenhag.rrp.platform.persistence.migrator import apply_migrations

_LOG = logging.getLogger(__name__)


def add_news_subparser(root_subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
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
        help="Pfad zu DuckDB (Default aus config.yaml: database.default_path oder 'data/pm.duckdb')",
    )
    p_harvest.add_argument("--rss-workers", type=int, default=4, help="Threads für RSS-Auflösung")
    p_harvest.add_argument(
        "--auto-migrate",
        action="store_true",
        help="Falls Schema fehlt: Migrationen automatisch anwenden (persistence/migrations)",
    )
    p_harvest.add_argument(
        "--dry-run",
        action="store_true",
        help="Simuliere Harvest ohne Persistenz (für Tests)",
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
    if not auto_migrate:
        _LOG.info("Skipping schema check as --auto-migrate is not set")
        return
    try:
        repo = DuckDBNewsRepository(db_path=db_path)
        with repo._connect() as con:
            tables = con.execute("SHOW TABLES").fetchall()
            table_names = [row[0] for row in tables]
            if "assets" not in table_names:
                migrations_path = Path("src/ch/bfh/pm/platform/persistence/migrations")
                applied = apply_migrations(db_path, str(migrations_path))
                if applied:
                    _LOG.info(f"[migrate] Applied: {', '.join(applied)}")
                else:
                    raise RuntimeError("Schema initialization failed")
            else:
                _LOG.info("Schema OK")
    except duckdb.IOException as e:
        raise SystemExit(f"Database error: {e}") from e
    except Exception as e:
        _LOG.warning(f"Schema check failed ({e}), trying migrations")
        migrations_path = Path("src/ch/bfh/pm/platform/persistence/migrations")
        applied = apply_migrations(db_path, str(migrations_path))
        if applied:
            _LOG.info(f"[migrate] Applied: {', '.join(applied)}")
        else:
            raise SystemExit("Schema initialization failed")


def _cmd_news_harvest(args: argparse.Namespace, *, config: Settings, metrics: Metrics) -> None:
    _ensure_schema(args.db, args.auto_migrate)

    asset = args.asset.upper()
    start, end = _build_time_range(args.days, args.date_from, args.date_to)
    criteria = HarvestCriteriaDTO(asset_symbol=asset, start=start, end=end, limit=args.limit)

    repo = DuckDBNewsRepository(db_path=args.db)
    domain_repo = DuckDBDomainPolicyRepository(db_path=args.db)
    domain_policy = DomainPolicyAdapter(domain_repo)
    registry = DuckDBAssetRegistryRepository(db_path=args.db)
    query_builder = NewsQueryBuilder(asset_registry=registry)  # Dynamische Aliases/Negatives

    enforce = bool(config.get("news_domain_filter", "enforce", False))

    sources = NewsSourceFactory(config, metrics).create_sources(args.source, args.rss_workers)
    svc = HarvestUrls(
        sources=sources,
        repo=repo,
        max_workers=int(config.get("url_harvest", "max_workers", 4)),
        domain_policy=domain_policy,
        enforce_domain_filter=enforce,
    )

    # Dry-Run: Skip Persist, log Queries
    if args.dry_run:
        _LOG.info(f"[dry-run] Query for {asset}: {query_builder.build_core_boolean(asset)}")
        _LOG.info("[dry-run] Skipping persist – simulate summary")
        print("[news/harvest] dry-run: total=0 assembled=0 deduped=0 saved=0 duplicates=0 rejected=0")
        return

    start_time = time.time()
    summary = svc.run(criteria=criteria, verbose=True, progress_every=25)
    metrics.track_harvest_duration(asset, time.time() - start_time)
    print(
        f"[news/harvest] asset={asset} "
        f"total={summary.total_docs} assembled={summary.after_assemble} "
        f"deduped={summary.after_dedupe} saved={summary.saved} "
        f"duplicates={summary.skipped_duplicates} rejected={summary.rejected_invalid}"
    )