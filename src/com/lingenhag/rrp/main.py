# src/com/lingenhag/rrp/main.py
from __future__ import annotations

import argparse
import logging
import sys

from com.lingenhag.rrp.platform.config.settings import Settings
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.news.presentation.cli_commands import add_news_subparser
from com.lingenhag.rrp.features.market.presentation.cli_commands import add_market_subparser
# Wichtig: _build_llm_from_config NICHT auf Modulebene importieren → Lazy-Import im LLM-Zweig
from com.lingenhag.rrp.features.llm.presentation.cli_commands import add_llm_subparser

from com.lingenhag.rrp.features.market.infrastructure.coingecko_client import CoinGeckoClient
from com.lingenhag.rrp.features.market.infrastructure.repositories.duckdb_market_repository import (
    DuckDBMarketRepository,
)

logging.basicConfig(level=logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    """
    Root-CLI für modulare Slices (News/Market/LLM).
    Beispiel:
      rrp news harvest --asset DOT --days 1
      rrp market factors --asset BTC --days 365
      rrp llm process --asset ETH --days 1
    """
    parser = argparse.ArgumentParser(prog="rrp", description="com.lingenhag.rrp – Modular CLI")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Pfad zur Konfigurationsdatei (Default: config.yaml)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8000,
        help="Prometheus Metrics Port (Default: 8000)",
    )

    subparsers = parser.add_subparsers(dest="feature", required=True)

    # ---- News-Slice ----
    add_news_subparser(subparsers)

    # ---- Market-Slice ----
    add_market_subparser(subparsers)

    # ---- LLM-Slice ----
    add_llm_subparser(subparsers)

    return parser


def _resolve_db_path(config: Settings, args_db: str | None) -> str:
    """
    Einheitliche Default-DB-Auflösung für alle Slices:
    - CLI-Argument --db hat Vorrang
    - sonst config.yaml → database.default_path
    - Fallback: data/rrp.duckdb
    """
    if args_db and str(args_db).strip():
        return str(args_db)
    return str(config.get("database", "default_path", "data/rrp.duckdb"))


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = Settings.load(args.config)
    metrics = Metrics(port=args.metrics_port)
    metrics.start_server()

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(2)

    # LLM-Slice: Lazy-Import, damit ImportError vermieden wird,
    # falls die Factory-Funktion umbenannt/verschoben ist.
    if args.feature == "llm":
        # DB-Pfad auflösen (optional)
        db_path = _resolve_db_path(config, getattr(args, "db", None))
        setattr(args, "db", db_path)

        # Lazy import hier – verhindert ImportError beim Laden von main.py
        from com.lingenhag.rrp.features.llm.presentation.cli_commands import (  # type: ignore[import-not-found]
            _build_llm_from_config,
        )

        llm = _build_llm_from_config(config, metrics)
        args.func(args, config=config, metrics=metrics, llm=llm)
        return

    # Market-Slice: Source (CoinGeckoClient) + Repository injizieren
    if args.feature == "market":
        source = CoinGeckoClient(
            api_base=str(config.get("coingecko", "api_base", "https://api.coingecko.com/api/v3")),
            api_key=config.get("coingecko", "api_key", None),
            timeout=int(config.get("coingecko", "timeout", 20)),
            max_retries=int(config.get("coingecko", "max_retries", 3)),
            initial_backoff=float(config.get("coingecko", "initial_backoff", 1.0)),
            metrics=metrics,
        )
        db_path = _resolve_db_path(config, getattr(args, "db", None))
        setattr(args, "db", db_path)
        repo = DuckDBMarketRepository(db_path=db_path)
        args.func(args, config=config, metrics=metrics, source=source, repo=repo)
        return

    # News-Slice: nur DB-Default setzen und ausführen
    if args.feature == "news":
        db_path = _resolve_db_path(config, getattr(args, "db", None))
        setattr(args, "db", db_path)
        args.func(args, config=config, metrics=metrics)
        return

    # Fallback-Dispatch (falls ein Subparser eigene Signatur hat)
    args.func(args, config=config, metrics=metrics)


if __name__ == "__main__":
    main()