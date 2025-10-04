# src/com/lingenhag/rrp/features/market/presentation/cli_commands.py
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, List

import duckdb

from com.lingenhag.rrp.platform.config.settings import Settings
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.platform.persistence.migrator import apply_migrations
from com.lingenhag.rrp.features.market.application.usecases.ingest_spot import IngestSpot
from com.lingenhag.rrp.features.market.application.usecases.ingest_history_range import IngestHistoryRange
from com.lingenhag.rrp.features.market.application.usecases.compute_market_factors import ComputeMarketFactors
from com.lingenhag.rrp.features.market.application.usecases.dashboard_queries import DashboardQueries
from com.lingenhag.rrp.features.market.application.ports import MarketDataPort, MarketRepositoryPort


def add_market_subparser(root_subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    market_parser = root_subparsers.add_parser("market", help="Market data processing")
    market_sub = market_parser.add_subparsers(dest="market_cmd", required=True)

    # Spot ingest
    p_spot = market_sub.add_parser("ingest", help="Ingest spot market data")
    p_spot.add_argument("--asset", nargs="+", required=True, help="Asset symbol(s), e.g., BTC ETH SOL")
    p_spot.add_argument("--vs", default="usd", help="Currency (default: usd)")
    p_spot.add_argument("--provider", default="CoinGecko", help="Data provider (default: CoinGecko)")
    p_spot.add_argument("--provider-id", help="Provider-specific ID (optional, single asset only)")
    p_spot.add_argument(
        "--db",
        help="Path to DuckDB (Default aus config.yaml: database.default_path oder 'data/rrp.duckdb')",
    )
    p_spot.add_argument("--auto-migrate", action="store_true", help="Apply migrations if schema is missing")
    p_spot.set_defaults(func=_cmd_ingest_spot)

    # History ingest
    p_history = market_sub.add_parser("history", help="Ingest historical market data")
    p_history.add_argument("--asset", required=True, help="Asset symbol (e.g., BTC)")
    p_history.add_argument("--vs", default="usd", help="Currency (default: usd)")
    p_history.add_argument("--provider", default="CoinGecko", help="Data provider (default: CoinGecko)")
    p_history.add_argument("--provider-id", help="Provider-specific ID (optional)")
    p_history.add_argument("--days", type=int, default=30, help="Days to fetch (default: 30)")
    p_history.add_argument("--from-ts", type=int, help="Start timestamp (Unix)")
    p_history.add_argument("--to-ts", type=int, help="End timestamp (Unix)")
    p_history.add_argument(
        "--db",
        help="Path to DuckDB (Default aus config.yaml: database.default_path oder 'data/rrp.duckdb')",
    )
    p_history.add_argument("--auto-migrate", action="store_true", help="Apply migrations if schema is missing")
    p_history.set_defaults(func=_cmd_ingest_history)

    # Factors
    p_factors = market_sub.add_parser(
        "factors",
        help="Compute and persist daily market factors (Quant × Sentiment). Includes Sharpe/Sortino/VaR/Pα.",
    )
    p_factors.add_argument("--asset", required=True, help="Asset symbol (e.g., BTC)")
    p_factors.add_argument("--days", type=int, default=365, help="How many days back (default: 365)")
    p_factors.add_argument("--start", help="ISO start date (YYYY-MM-DD), overrides --days")
    p_factors.add_argument("--end", help="ISO end date (YYYY-MM-DD), defaults to today (UTC)")
    p_factors.add_argument("--alpha", type=float, default=0.25, help="Blend factor α for Pα (default: 0.25)")
    p_factors.add_argument("--window-vol", type=int, default=30, help="Rolling window for Vol/Sharpe/Sortino/VaR")
    p_factors.add_argument("--window-sent", type=int, default=90, help="Rolling window for sentiment normalization")
    p_factors.add_argument("--ema-len", type=int, default=30, help="EMA length for expected return")
    p_factors.add_argument("--norm", choices=["zscore", "winsor", "minmax"], default="zscore")
    p_factors.add_argument("--winsor-alpha", type=float, default=0.05, help="Winsor alpha lower/upper tail")
    p_factors.add_argument("--var", choices=["param95", "emp95"], default="param95")
    p_factors.add_argument("--export", metavar="PATH", help="Optional CSV export of computed rows")
    p_factors.add_argument("--db", help="Path to DuckDB (Default aus config.yaml: database.default_path)")
    p_factors.add_argument("--auto-migrate", action="store_true", help="Apply migrations if schema is missing")
    p_factors.add_argument("--dry-run", action="store_true", help="Compute only, do not persist")
    p_factors.set_defaults(func=_cmd_compute_factors)

    # Dashboard overview
    p_over = market_sub.add_parser("overview", help="Show basic market KPIs for an asset and date range")
    p_over.add_argument("--asset", required=True, help="Asset symbol (e.g., BTC)")
    p_over.add_argument("--start", required=True, help="ISO start date (YYYY-MM-DD)")
    p_over.add_argument("--end", required=True, help="ISO end date (YYYY-MM-DD)")
    p_over.add_argument("--vs", default="usd", help="Currency (default: usd) — aligns with stored candles")
    p_over.add_argument("--format", choices=["table", "json"], default="table", help="Output format")
    p_over.add_argument(
        "--db",
        help="Path to DuckDB (Default aus config.yaml: database.default_path oder 'data/rrp.duckdb')",
    )
    p_over.add_argument("--auto-migrate", action="store_true", help="Apply migrations if schema is missing")
    p_over.set_defaults(func=_cmd_market_overview)


def _parse_iso(value: str) -> datetime:
    try:
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        raise SystemExit(f"Ungültiges Datumsformat: {value}") from e


def _build_time_range(days: int, from_ts: Optional[int], to_ts: Optional[int]) -> tuple[datetime, datetime]:
    if from_ts is not None and to_ts is not None:
        if from_ts >= to_ts:
            raise SystemExit("--from-ts muss kleiner als --to-ts sein")
        start = datetime.fromtimestamp(from_ts, tz=timezone.utc)
        end = datetime.fromtimestamp(to_ts, tz=timezone.utc)
        return start, end
    if from_ts or to_ts:
        raise SystemExit("Bitte beide angeben (--from-ts und --to-ts) oder keins")
    if days < 1:
        raise SystemExit("--days muss positiv sein")
    now = datetime.now(timezone.utc)
    return now - timedelta(days=days), now


def _ensure_schema(db_path: str, auto_migrate: bool) -> None:
    if not auto_migrate:
        return
    try:
        with duckdb.connect(db_path) as con:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        required = {
            "assets",
            "asset_providers",
            "market_history",
            "market_snapshots",
            "summarized_articles",
            "llm_votes",
            "rejections",
        }
        if not required.issubset(tables):
            migrations_path = Path("src/com/lingenhag/rrp/platform/persistence/migrations")
            applied = apply_migrations(db_path, str(migrations_path))
            if not applied:
                print("[migrations] Keine Migrationen angewendet (vermutlich bereits aktuell).")
    except duckdb.IOException as e:
        raise SystemExit(f"Database error: {e}") from e


def _cmd_ingest_spot(
        args: argparse.Namespace,
        config: Settings,
        metrics: Metrics,
        source: MarketDataPort,
        repo: MarketRepositoryPort,
) -> None:
    _ensure_schema(args.db, args.auto_migrate)
    assets: List[str] = [a.upper() for a in args.asset]
    pairs = repo.list_provider_pairs(provider=args.provider, asset_symbols=assets)
    known = {sym for sym, _ in pairs}
    for sym in assets:
        if sym not in known:
            pid = args.provider_id if len(assets) == 1 and args.provider_id else sym.lower()
            repo.upsert_asset_provider(asset_symbol=sym, provider=args.provider, provider_id=pid)
            pairs.append((sym, pid))
    svc = IngestSpot(repo=repo, source=source)
    res = svc.execute(assets=pairs, vs_currency=args.vs)
    print(
        f"[market-spot] assets={pairs} requested={res.requested} "
        f"fetched={res.fetched} saved={res.saved} duplicates={res.duplicates}"
    )


def _cmd_ingest_history(
        args: argparse.Namespace,
        config: Settings,
        metrics: Metrics,
        source: MarketDataPort,
        repo: MarketRepositoryPort,
) -> None:
    _ensure_schema(args.db, args.auto_migrate)
    asset_sym = args.asset.upper()
    start, end = _build_time_range(args.days, args.from_ts, args.to_ts)
    provider_id = args.provider_id or repo.get_provider_id(asset_symbol=asset_sym, provider=args.provider) or asset_sym.lower()
    svc = IngestHistoryRange(repo=repo, source=source)
    res = svc.execute(
        asset_symbol=asset_sym,
        provider_id=provider_id,
        from_ts=int(start.timestamp()),
        to_ts=int(end.timestamp()),
        vs_currency=args.vs,
    )
    print(
        f"[market-history] asset={asset_sym} provider_id={provider_id} "
        f"fetched={res.fetched} saved={res.saved} duplicates={res.duplicates}"
    )


def _cmd_compute_factors(
        args: argparse.Namespace,
        config: Settings,
        metrics: Metrics,
        source: MarketDataPort,  # unused in this command
        repo: MarketRepositoryPort,
) -> None:
    _ensure_schema(args.db, args.auto_migrate)

    asset = args.asset.upper()
    if args.start:
        start_dt = _parse_iso(args.start)
        start_d = start_dt.date()
    else:
        start_d = (datetime.now(timezone.utc) - timedelta(days=int(args.days))).date()

    if args.end:
        end_dt = _parse_iso(args.end)
        end_d = end_dt.date()
    else:
        end_d = datetime.now(timezone.utc).date()

    svc = ComputeMarketFactors(
        repo=repo,
        window_vol=int(args.window_vol),
        window_sent=int(args.window_sent),
        ema_len=int(args.ema_len),
        norm_method=str(args.norm),
        winsor_alpha=float(getattr(args, "winsor_alpha", 0.05)),
        var_method=str(args.var),
    )
    result = svc.execute(asset_symbol=asset, start=start_d, end=end_d, alpha=float(args.alpha), persist=not args.dry_run)

    saved = result.inserted if not args.dry_run else 0
    updated = result.updated if not args.dry_run else 0

    print(
        f"[market-factors] asset={asset} range={start_d}..{end_d} "
        f"saved={saved} updated={updated} alpha={args.alpha} dry_run={args.dry_run} "
        f"vol_win={args.window_vol} sent_win={args.window_sent} ema_len={args.ema_len} "
        f"norm={args.norm} var={args.var}"
    )

    if args.export:
        fieldnames = [
            "asset_symbol",
            "day",
            "ret_1d",
            "vol_30d",
            "sharpe_30d",
            "sortino_30d",
            "var_1d_95",
            "exp_return_30d",
            "sentiment_mean",
            "sentiment_norm",
            "p_alpha",
            "alpha",
        ]
        with open(args.export, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in result.rows:
                writer.writerow(
                    {
                        "asset_symbol": r.asset_symbol,
                        "day": r.day.isoformat(),
                        "ret_1d": r.ret_1d,
                        "vol_30d": r.vol_30d,
                        "sharpe_30d": r.sharpe_30d,
                        "sortino_30d": r.sortino_30d,
                        "var_1d_95": r.var_1d_95,
                        "exp_return_30d": r.exp_return_30d,
                        "sentiment_mean": r.sentiment_mean,
                        "sentiment_norm": r.sentiment_norm,
                        "p_alpha": r.p_alpha,
                        "alpha": r.alpha,
                    }
                )
        print(f"[market-factors] exported CSV → {args.export}")


def _cmd_market_overview(
        args: argparse.Namespace,
        config: Settings,
        metrics: Metrics,
        source: MarketDataPort,  # unused, kept for signature consistency
        repo: MarketRepositoryPort,
) -> None:
    _ensure_schema(args.db, args.auto_migrate)
    asset = args.asset.upper()
    start_d = _parse_iso(args.start).date()
    end_d = _parse_iso(args.end).date()

    dq = DashboardQueries(repo=repo)
    ov = dq.market_overview(asset_symbol=asset, start=start_d, end=end_d)

    # Zusatzmetriken aus Candles (kein weiterer DB-Zugriff)
    candles = repo.fetch_range(asset_symbol=asset, provider="CoinGecko", vs_currency=args.vs, start=start_d, end=end_d)
    n_days = len(candles)
    first_close = candles[0].close if candles else None
    last_close = candles[-1].close if candles else None
    ret_period = ((last_close / first_close) - 1.0) if (first_close and last_close) else None

    if args.format == "json":
        out = {
            "asset": ov.asset_symbol,
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "days": n_days,
            "latest_close": ov.latest_close,
            "avg_volume": ov.avg_volume,
            "avg_market_cap": ov.avg_market_cap,
            "return_period": ret_period,
        }
        print(json.dumps(out, indent=2))
    else:
        print("[market-overview]")
        print(f"  asset         : {ov.asset_symbol}")
        print(f"  range         : {start_d} .. {end_d}  (days={n_days})")
        print(f"  latest_close  : {ov.latest_close:,.2f}")
        print(f"  avg_volume    : {ov.avg_volume:,.2f}")
        print(f"  avg_market_cap: {ov.avg_market_cap:,.2f}")
        if ret_period is not None:
            print(f"  return_period : {ret_period*100:,.2f}%")