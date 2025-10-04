# src/com/lingenhag/rrp/platform/persistence/migrator.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import duckdb  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

def _split_sql(sql: str) -> List[str]:
    """
    Simple statement splitter: Splits on ';' at line ends.
    Ignores empty/whitespace blocks. Sufficient for our migration files.
    """
    parts: List[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        buf.append(line)
        if line.strip().endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                parts.append(stmt)
            buf = []
    # Trailing without semicolon
    tail = "\n".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts

def _init_migrations_table(con) -> None:
    """Initialize tracking table for applied migrations."""
    con.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                                                          filename TEXT PRIMARY KEY,
                                                          applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)

def apply_migrations(db_path: str, migrations_dir: str) -> List[str]:
    """
    Applies SQL migrations file-by-file, statement-by-statement with clear errors.
    Tracks applied migrations in 'migrations' table; skips existing.
    """
    migrations_path = Path(migrations_dir)
    applied: List[str] = []

    with duckdb.connect(db_path) as con:
        con.execute("SET TimeZone='UTC'")
        _init_migrations_table(con)

        for migration_file in sorted(migrations_path.glob("*.sql")):
            filename = migration_file.name
            # Check if already applied
            if con.execute("SELECT 1 FROM migrations WHERE filename = ?", [filename]).fetchone():
                logger.info(f"Skipping applied migration: {filename}")
                continue

            with open(migration_file, "r", encoding="utf-8") as f:
                sql = f.read()
            statements = _split_sql(sql)
            try:
                for idx, stmt in enumerate(statements, start=1):
                    con.execute(stmt)
                # Mark as applied
                con.execute("INSERT INTO migrations (filename) VALUES (?)", [filename])
                applied.append(filename)
                logger.info(f"Applied migration: {filename}")
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(
                    f"Migration '{filename}' failed at statement #{idx}:\n{stmt}\nError: {e}"
                ) from e

    return applied