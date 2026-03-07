from datetime import date, timedelta

from .db import get_db


def ensure_default_athlete(name="Demo Athlete"):
    db = get_db()
    row = db.execute("SELECT athlete_id FROM athletes ORDER BY athlete_id LIMIT 1").fetchone()
    if row:
        return row["athlete_id"]

    cursor = db.execute("INSERT INTO athletes (name) VALUES (?)", (name,))
    db.commit()
    return cursor.lastrowid


def create_athlete(name):
    db = get_db()
    cursor = db.execute("INSERT INTO athletes (name) VALUES (?)", (name.strip(),))
    db.commit()
    return cursor.lastrowid


def fetch_all_athletes():
    db = get_db()
    return db.execute("SELECT * FROM athletes ORDER BY name ASC").fetchall()


def fetch_athlete(athlete_id):
    db = get_db()
    return db.execute("SELECT * FROM athletes WHERE athlete_id = ?", (athlete_id,)).fetchone()


def update_athlete_name(athlete_id, name):
    db = get_db()
    db.execute("UPDATE athletes SET name = ? WHERE athlete_id = ?", (name.strip(), athlete_id))
    db.commit()


def insert_goal(athlete_id, event_name, distance_km, goal_time, race_date, elevation_type):
    db = get_db()
    db.execute(
        "UPDATE goals SET status = 'ARCHIVED' WHERE athlete_id = ? AND status = 'ACTIVE'",
        (athlete_id,),
    )
    db.execute(
        """
        INSERT INTO goals (athlete_id, event_name, distance_km, goal_time, race_date, elevation_type, status)
        VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')
        """,
        (athlete_id, event_name, distance_km, goal_time, race_date, elevation_type),
    )
    db.commit()


def fetch_active_goal(athlete_id):
    db = get_db()
    return db.execute(
        """
        SELECT * FROM goals
        WHERE athlete_id = ? AND status = 'ACTIVE'
        ORDER BY goal_id DESC
        LIMIT 1
        """,
        (athlete_id,),
    ).fetchone()


def save_metrics(
    athlete_id,
    activity_id,
    timestamp,
    distance_km,
    moving_time_sec,
    avg_hr,
    elevation_gain_m,
    stress,
    atl,
    ctl,
    tsb,
    readiness,
):
    db = get_db()
    db.execute(
        """
        INSERT OR REPLACE INTO daily_metrics
        (activity_id, athlete_id, timestamp, distance_km, moving_time_sec, avg_hr, elevation_gain_m,
         stress, atl, ctl, tsb, readiness)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            activity_id,
            athlete_id,
            timestamp,
            distance_km,
            moving_time_sec,
            avg_hr,
            elevation_gain_m,
            stress,
            atl,
            ctl,
            tsb,
            readiness,
        ),
    )
    db.commit()


def fetch_latest_metric(athlete_id):
    db = get_db()
    return db.execute(
        """
        SELECT * FROM daily_metrics
        WHERE athlete_id = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (athlete_id,),
    ).fetchone()


def fetch_all_metrics(athlete_id):
    db = get_db()
    return db.execute(
        """
        SELECT * FROM daily_metrics
        WHERE athlete_id = ?
        ORDER BY timestamp ASC
        """,
        (athlete_id,),
    ).fetchall()


def fetch_recent_metrics(athlete_id, limit=30):
    db = get_db()
    return db.execute(
        """
        SELECT * FROM daily_metrics
        WHERE athlete_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (athlete_id, limit),
    ).fetchall()


def log_health_status(athlete_id, resting_hr, hr_delta, illness_flag, override_active):
    db = get_db()
    today = date.today().isoformat()
    db.execute(
        """
        INSERT OR REPLACE INTO health_status
        (athlete_id, log_date, resting_hr, hr_delta, illness_flag, override_active)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (athlete_id, today, resting_hr, hr_delta, illness_flag, override_active),
    )
    db.commit()


def save_prescribed_session(athlete_id, session_date, session_type, min_km, max_km, intensity):
    db = get_db()
    db.execute(
        """
        INSERT OR REPLACE INTO prescribed_sessions
        (athlete_id, session_date, session_type, min_km, max_km, intensity)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (athlete_id, session_date, session_type, min_km, max_km, intensity),
    )
    db.commit()


def fetch_today_prescription(athlete_id):
    db = get_db()
    today = date.today().isoformat()
    return db.execute(
        """
        SELECT * FROM prescribed_sessions
        WHERE athlete_id = ? AND session_date = ?
        """,
        (athlete_id, today),
    ).fetchone()


def update_compliance(athlete_id, session_date, score):
    db = get_db()
    db.execute(
        """
        UPDATE prescribed_sessions
        SET completed = 1, compliance_score = ?
        WHERE athlete_id = ? AND session_date = ?
        """,
        (score, athlete_id, session_date),
    )
    db.commit()


def weekly_compliance(athlete_id):
    db = get_db()
    monday = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    sunday = (date.today() + timedelta(days=6 - date.today().weekday())).isoformat()
    rows = db.execute(
        """
        SELECT compliance_score FROM prescribed_sessions
        WHERE athlete_id = ? AND session_date BETWEEN ? AND ?
        """,
        (athlete_id, monday, sunday),
    ).fetchall()

    if not rows:
        return 0.0

    values = [row["compliance_score"] for row in rows if row["compliance_score"] is not None]
    if not values:
        return 0.0
    return round(sum(values) / len(values), 1)


def save_strava_account(
    athlete_id,
    strava_athlete_id,
    refresh_token,
    access_token,
    access_expires_at,
):
    db = get_db()
    db.execute(
        """
        INSERT INTO strava_accounts
        (athlete_id, strava_athlete_id, refresh_token, access_token, access_expires_at, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(athlete_id)
        DO UPDATE SET
            strava_athlete_id = excluded.strava_athlete_id,
            refresh_token = excluded.refresh_token,
            access_token = excluded.access_token,
            access_expires_at = excluded.access_expires_at,
            updated_at = CURRENT_TIMESTAMP
        """,
        (athlete_id, strava_athlete_id, refresh_token, access_token, access_expires_at),
    )
    db.commit()


def get_strava_account(athlete_id):
    db = get_db()
    return db.execute(
        "SELECT * FROM strava_accounts WHERE athlete_id = ?",
        (athlete_id,),
    ).fetchone()
