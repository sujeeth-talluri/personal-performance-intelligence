from .db import get_db


def create_user(name, email, password_hash):
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO users (name, email, password_hash)
        VALUES (?, ?, ?)
        """,
        (name.strip(), email.strip().lower(), password_hash),
    )
    db.commit()
    return cursor.lastrowid


def get_user_by_email(email):
    db = get_db()
    return db.execute(
        "SELECT * FROM users WHERE email = ?",
        (email.strip().lower(),),
    ).fetchone()


def get_user_by_id(user_id):
    db = get_db()
    return db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def update_user_name(user_id, name):
    db = get_db()
    db.execute("UPDATE users SET name = ? WHERE id = ?", (name.strip(), user_id))
    db.commit()


def save_strava_tokens(user_id, strava_athlete_id, access_token, refresh_token, expires_at):
    db = get_db()
    db.execute(
        """
        UPDATE users
        SET strava_athlete_id = ?,
            strava_access_token = ?,
            strava_refresh_token = ?,
            strava_access_expires_at = ?
        WHERE id = ?
        """,
        (strava_athlete_id, access_token, refresh_token, expires_at, user_id),
    )
    db.commit()


def get_goal(user_id):
    db = get_db()
    return db.execute(
        "SELECT * FROM goals WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user_id,),
    ).fetchone()


def save_goal(user_id, race_name, race_distance, goal_time, race_date, elevation_type, current_pb):
    db = get_db()
    existing = get_goal(user_id)
    if existing:
        db.execute(
            """
            UPDATE goals
            SET race_name = ?, race_distance = ?, goal_time = ?, race_date = ?,
                elevation_type = ?, current_pb = ?
            WHERE id = ?
            """,
            (race_name, race_distance, goal_time, race_date, elevation_type, current_pb, existing["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO goals (user_id, race_name, race_distance, goal_time, race_date, elevation_type, current_pb)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, race_name, race_distance, goal_time, race_date, elevation_type, current_pb),
        )
    db.commit()


def save_metrics(
    user_id,
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
        (activity_id, user_id, athlete_id, timestamp, distance_km, moving_time_sec, avg_hr, elevation_gain_m,
         stress, atl, ctl, tsb, readiness)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            activity_id,
            user_id,
            user_id,
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


def fetch_latest_metric(user_id):
    db = get_db()
    return db.execute(
        "SELECT * FROM daily_metrics WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1",
        (user_id,),
    ).fetchone()


def fetch_all_metrics(user_id):
    db = get_db()
    return db.execute(
        "SELECT * FROM daily_metrics WHERE user_id = ? ORDER BY timestamp ASC",
        (user_id,),
    ).fetchall()


def fetch_recent_metrics(user_id, limit=30):
    db = get_db()
    return db.execute(
        "SELECT * FROM daily_metrics WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
        (user_id, limit),
    ).fetchall()
