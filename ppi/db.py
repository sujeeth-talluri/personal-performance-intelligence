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


def initialize_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS athletes (
            athlete_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS strava_accounts (
            athlete_id INTEGER PRIMARY KEY,
            strava_athlete_id INTEGER,
            refresh_token TEXT,
            access_token TEXT,
            access_expires_at INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS goals (
            goal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            athlete_id INTEGER NOT NULL,
            event_name TEXT NOT NULL,
            distance_km REAL NOT NULL,
            goal_time TEXT NOT NULL,
            race_date TEXT NOT NULL,
            elevation_type TEXT NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            activity_id INTEGER,
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
            PRIMARY KEY (activity_id, athlete_id),
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS health_status (
            athlete_id INTEGER,
            log_date TEXT,
            resting_hr INTEGER,
            hr_delta REAL,
            illness_flag INTEGER DEFAULT 0,
            override_active INTEGER DEFAULT 0,
            PRIMARY KEY (athlete_id, log_date),
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS prescribed_sessions (
            athlete_id INTEGER,
            session_date TEXT,
            session_type TEXT,
            min_km REAL,
            max_km REAL,
            intensity TEXT,
            completed INTEGER DEFAULT 0,
            compliance_score REAL DEFAULT 0,
            PRIMARY KEY (athlete_id, session_date),
            FOREIGN KEY (athlete_id) REFERENCES athletes (athlete_id)
        )
        """
    )

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_daily_metrics_athlete_time ON daily_metrics (athlete_id, timestamp)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_goals_athlete_status ON goals (athlete_id, status)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_strava_accounts_external ON strava_accounts (strava_athlete_id)"
    )

    conn.commit()
    conn.close()


def get_last_7_day_hr_avg(athlete_id):
    db = get_db()
    seven_days_ago = (date.today() - timedelta(days=7)).isoformat()
    rows = db.execute(
        """
        SELECT resting_hr FROM health_status
        WHERE athlete_id = ? AND log_date >= ?
        """,
        (athlete_id, seven_days_ago),
    ).fetchall()

    if not rows:
        return None

    return sum(row["resting_hr"] for row in rows) / len(rows)
