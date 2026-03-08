import sqlite3
from datetime import date, timedelta

from flask import current_app, g


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE_PATH"])
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(_exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _table_columns(cursor, table_name):
    rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def initialize_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            strava_athlete_id INTEGER,
            strava_access_token TEXT,
            strava_refresh_token TEXT,
            strava_access_expires_at INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Recreate goals table with product schema if legacy structure exists.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS goals_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            race_name TEXT NOT NULL,
            race_distance REAL NOT NULL,
            goal_time TEXT NOT NULL,
            race_date TEXT NOT NULL,
            elevation_type TEXT NOT NULL,
            current_pb TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """
    )

    if cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='goals'").fetchone():
        goal_cols = _table_columns(cursor, "goals")
        if "user_id" not in goal_cols or "race_name" not in goal_cols:
            cursor.execute("DROP TABLE IF EXISTS goals_legacy")
            cursor.execute("ALTER TABLE goals RENAME TO goals_legacy")
            cursor.execute(
                """
                INSERT INTO goals_new (user_id, race_name, race_distance, goal_time, race_date, elevation_type)
                SELECT athlete_id, event_name, distance_km, goal_time, race_date, elevation_type
                FROM goals_legacy
                """
            )
            cursor.execute("DROP TABLE goals_legacy")
        else:
            cursor.execute(
                """
                INSERT INTO goals_new (id, user_id, race_name, race_distance, goal_time, race_date, elevation_type, current_pb, created_at)
                SELECT id, user_id, race_name, race_distance, goal_time, race_date, elevation_type, current_pb, COALESCE(created_at, CURRENT_TIMESTAMP)
                FROM goals
                """
            )
            cursor.execute("DROP TABLE goals")
    cursor.execute("ALTER TABLE goals_new RENAME TO goals")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            activity_id INTEGER,
            user_id INTEGER,
            athlete_id INTEGER,
            timestamp TEXT,
            distance_km REAL,
            moving_time_sec REAL,
            avg_hr REAL,
            elevation_gain_m REAL,
            stress REAL,
            atl REAL,
            ctl REAL,
            tsb REAL,
            readiness REAL,
            PRIMARY KEY (activity_id, athlete_id)
        )
        """
    )

    metric_cols = _table_columns(cursor, "daily_metrics")
    if "user_id" not in metric_cols:
        cursor.execute("ALTER TABLE daily_metrics ADD COLUMN user_id INTEGER")
    if "athlete_id" not in metric_cols:
        cursor.execute("ALTER TABLE daily_metrics ADD COLUMN athlete_id INTEGER")
    cursor.execute("UPDATE daily_metrics SET user_id = COALESCE(user_id, athlete_id)")
    cursor.execute("UPDATE daily_metrics SET athlete_id = COALESCE(athlete_id, user_id)")

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_daily_metrics_user_time ON daily_metrics (user_id, timestamp)"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_goals_user ON goals (user_id)")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email)")

    conn.commit()
    conn.close()


def get_last_7_day_hr_avg(user_id):
    db = get_db()
    seven_days_ago = (date.today() - timedelta(days=7)).isoformat()
    rows = db.execute(
        """
        SELECT avg_hr FROM daily_metrics
        WHERE user_id = ? AND timestamp >= ?
        """,
        (user_id, f"{seven_days_ago}T00:00:00Z"),
    ).fetchall()

    values = [float(row["avg_hr"]) for row in rows if row["avg_hr"] is not None]
    if not values:
        return None

    return sum(values) / len(values)
