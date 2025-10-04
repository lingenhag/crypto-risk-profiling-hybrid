# src/com/lingenhag/rrp/main.py
from __future__ import annotations

import argparse
import logging
import sys

from com.lingenhag.rrp.platform.config.settings import Settings
from com.lingenhag.rrp.platform.monitoring.metrics import Metrics
from com.lingenhag.rrp.features.news.presentation.cli_commands import add_news_subparser

logging.basicConfig(level=logging.INFO)


def build_parser() -> argparse.ArgumentParser:
    """
    Root-CLI für modulare Slices (News/LLM).
    Beispiel:
      rrp news harvest --asset DOT --days 1
      rrp llm process  --asset DOT --days 1
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

    # ---- LLM-Slice ---- (Lazy import, damit Importfehler beim Laden vermieden werden)
    from com.lingenhag.rrp.features.llm.presentation.cli_commands import add_llm_subparser
    add_llm_subparser(subparsers)

    return parser


def _resolve_db_path(config: Settings, args_db: str | None) -> str:
    """
    Einheitliche Default-DB-Auflösung:
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

    # LLM-Slice: LLM-Factory lazy importieren und DB-Path setzen
    if args.feature == "llm":
        db_path = _resolve_db_path(config, getattr(args, "db", None))
        setattr(args, "db", db_path)

        # Factory erst hier importieren
        from com.lingenhag.rrp.features.llm.presentation.cli_commands import _build_llm_from_config  # type: ignore[import-not-found]

        llm = _build_llm_from_config(config, metrics)
        args.func(args, config=config, metrics=metrics, llm=llm)
        return

    # News-Slice: DB-Default setzen und ausführen
    if args.feature == "news":
        db_path = _resolve_db_path(config, getattr(args, "db", None))
        setattr(args, "db", db_path)
        args.func(args, config=config, metrics=metrics)
        return

    # Fallback
    args.func(args, config=config, metrics=metrics)


if __name__ == "__main__":
    main()