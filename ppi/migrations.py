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


def _migration_003_runner_profiles_coaching_plans() -> None:
    """Ensure runner_profiles and coaching_plans tables exist.

    These models were added after the 001_baseline migration was first applied
    on some deployments (e.g. the original Render PostgreSQL instance that was
    later migrated to Supabase). Running db.create_all() here is idempotent —
    SQLAlchemy skips tables that already exist.
    """
    from . import models  # noqa: F401

    db.create_all()

    # Also ensure workout_logs.source column exists (added after initial schema).
    inspector = sa.inspect(db.engine)
    if "workout_logs" in inspector.get_table_names():
        existing = {col["name"] for col in inspector.get_columns("workout_logs")}
        with db.engine.begin() as conn:
            if "source" not in existing:
                conn.execute(
                    text("ALTER TABLE workout_logs ADD COLUMN source VARCHAR(24) NOT NULL DEFAULT 'engine'")
                )
            if "notes" not in existing:
                conn.execute(
                    text("ALTER TABLE workout_logs ADD COLUMN notes VARCHAR(255)")
                )


def _migration_004_workout_logs_drop_unique_date() -> None:
    """Allow multiple workout log entries per user per day (double-days).

    Drops the uq_user_workout_date unique constraint and replaces it with a
    plain composite index so date-range queries stay fast without preventing
    athletes from logging e.g. a morning run + evening strength session.
    """
    with db.engine.begin() as conn:
        # Drop the unique constraint (PostgreSQL syntax).
        # IF EXISTS guard makes this safe to run on DBs that never had it.
        conn.execute(
            text(
                "ALTER TABLE workout_logs "
                "DROP CONSTRAINT IF EXISTS uq_user_workout_date"
            )
        )
        # Create a plain composite index (idempotent via IF NOT EXISTS).
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_workout_logs_user_date "
                "ON workout_logs (user_id, workout_date)"
            )
        )


MIGRATIONS: list[tuple[str, MigrationFn]] = [
    ("001_baseline", _migration_001_baseline),
    ("002_goal_pb_columns", _migration_002_goal_pb_columns),
    ("003_runner_profiles_coaching_plans", _migration_003_runner_profiles_coaching_plans),
    ("004_workout_logs_drop_unique_date", _migration_004_workout_logs_drop_unique_date),
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
