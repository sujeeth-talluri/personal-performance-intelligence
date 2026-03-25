from __future__ import annotations

from collections.abc import Callable

import sqlalchemy as sa
from sqlalchemy import text

from .extensions import db


MigrationFn = Callable[[], None]


def _migration_001_baseline() -> None:
    # Baseline migration for existing deployments and fresh databases.
    from . import models  # noqa: F401

    db.create_all()


def _migration_002_goal_pb_columns() -> None:
    inspector = sa.inspect(db.engine)
    existing = {column["name"] for column in inspector.get_columns("goals")}
    with db.engine.begin() as conn:
        for col in ("pb_5k", "pb_10k", "pb_hm"):
            if col in existing:
                continue
            conn.execute(
                text(f"ALTER TABLE goals ADD COLUMN {col} VARCHAR(16)")
            )


MIGRATIONS: list[tuple[str, MigrationFn]] = [
    ("001_baseline", _migration_001_baseline),
    ("002_goal_pb_columns", _migration_002_goal_pb_columns),
]


def _ensure_migrations_table() -> None:
    with db.engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(64) PRIMARY KEY,
                    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )


def _applied_versions() -> set[str]:
    with db.engine.begin() as conn:
        rows = conn.execute(text("SELECT version FROM schema_migrations"))
        return {row[0] for row in rows}


def _record_migration(version: str) -> None:
    with db.engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO schema_migrations (version)
                VALUES (:version)
                """
            ),
            {"version": version},
        )


def run_migrations() -> list[str]:
    _ensure_migrations_table()
    applied = _applied_versions()
    ran: list[str] = []

    for version, migration in MIGRATIONS:
        if version in applied:
            continue
        migration()
        _record_migration(version)
        ran.append(version)

    return ran
