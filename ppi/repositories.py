from datetime import date, datetime

from sqlalchemy.dialects.postgresql import insert as pg_insert

from .crypto import decrypt_token, encrypt_token
from .extensions import db
from .models import Activity, Goal, Metric, PasswordReset, PredictionHistory, StravaToken, User, WorkoutLog


def _commit():
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise


def create_user(name, email, password_hash):
    user = User(name=name.strip(), email=email.strip().lower(), password_hash=password_hash)
    db.session.add(user)
    _commit()
    return user.id


def get_user_by_email(email):
    return User.query.filter_by(email=email.strip().lower()).first()


def get_user_by_id(user_id):
    return db.session.get(User, user_id)


def update_user_name(user_id, name):
    user = get_user_by_id(user_id)
    if user:
        user.name = name.strip()
        _commit()


def save_goal(user_id, race_name, race_distance, goal_time, race_date, elevation_type,
              personal_best, pb_5k=None, pb_10k=None, pb_hm=None):
    goal = Goal.query.filter_by(user_id=user_id).order_by(Goal.id.desc()).first()
    race_date_obj = datetime.strptime(race_date, "%Y-%m-%d").date()

    if goal:
        goal.race_name = race_name
        goal.race_distance = race_distance
        goal.goal_time = goal_time
        goal.race_date = race_date_obj
        goal.elevation_type = elevation_type
        goal.personal_best = personal_best
        goal.pb_5k  = pb_5k  or None
        goal.pb_10k = pb_10k or None
        goal.pb_hm  = pb_hm  or None
    else:
        goal = Goal(
            user_id=user_id,
            race_name=race_name,
            race_distance=race_distance,
            goal_time=goal_time,
            race_date=race_date_obj,
            elevation_type=elevation_type,
            personal_best=personal_best,
            pb_5k=pb_5k   or None,
            pb_10k=pb_10k or None,
            pb_hm=pb_hm   or None,
        )
        db.session.add(goal)

    _commit()


def get_goal(user_id):
    return Goal.query.filter_by(user_id=user_id).order_by(Goal.id.desc()).first()


def _unix_to_datetime(ts) -> datetime:
    """Convert a Unix timestamp (int or float) to a naive UTC datetime."""
    return datetime.utcfromtimestamp(int(ts))


def save_strava_tokens(user_id, athlete_id, access_token, refresh_token, expires_at):
    """Persist Strava OAuth tokens. Tokens are encrypted at rest."""
    enc_access  = encrypt_token(access_token)
    enc_refresh = encrypt_token(refresh_token)
    expires_int = int(expires_at)  # keep as Unix timestamp — DB column is INTEGER
    token = StravaToken.query.filter_by(user_id=user_id).first()
    if token:
        token.athlete_id    = athlete_id
        token.access_token  = enc_access
        token.refresh_token = enc_refresh
        token.expires_at    = expires_int
    else:
        token = StravaToken(
            user_id=user_id,
            athlete_id=athlete_id,
            access_token=enc_access,
            refresh_token=enc_refresh,
            expires_at=expires_int,
        )
        db.session.add(token)
    _commit()


def get_strava_token(user_id):
    """Fetch Strava tokens and decrypt them in-memory before returning.

    Callers receive a StravaToken ORM object whose access_token and
    refresh_token attributes hold the raw plaintext values — no changes
    required in calling code. The decrypted values are never flushed back
    to the DB (no commit is called here).
    """
    token = StravaToken.query.filter_by(user_id=user_id).first()
    if token:
        token.access_token  = decrypt_token(token.access_token)
        token.refresh_token = decrypt_token(token.refresh_token)
    return token


def upsert_activity(
    user_id,
    strava_activity_id,
    date_utc,
    activity_type,
    distance_km,
    moving_time,
    avg_hr,
    elevation_gain,
    is_race,
):
    row = Activity.query.filter_by(user_id=user_id, strava_activity_id=strava_activity_id).first()
    if row:
        row.date = date_utc
        row.activity_type = activity_type
        row.distance_km = distance_km
        row.moving_time = moving_time
        row.avg_hr = avg_hr
        row.elevation_gain = elevation_gain
        row.is_race = is_race
    else:
        row = Activity(
            user_id=user_id,
            strava_activity_id=strava_activity_id,
            date=date_utc,
            activity_type=activity_type,
            distance_km=distance_km,
            moving_time=moving_time,
            avg_hr=avg_hr,
            elevation_gain=elevation_gain,
            is_race=is_race,
        )
        db.session.add(row)


def fetch_activities(user_id, limit=None):
    q = Activity.query.filter_by(user_id=user_id).order_by(Activity.date.asc())
    if limit:
        return q.limit(limit).all()
    return q.all()


def fetch_recent_activities(user_id, limit=5):
    return Activity.query.filter_by(user_id=user_id).order_by(Activity.date.desc()).limit(limit).all()


def fetch_latest_activity(user_id):
    return Activity.query.filter_by(user_id=user_id).order_by(Activity.date.desc()).first()


def upsert_metric(user_id, metric_date, stress, atl, ctl, tsb):
    row = Metric.query.filter_by(user_id=user_id, date=metric_date).first()
    if row:
        row.stress = stress
        row.atl = atl
        row.ctl = ctl
        row.tsb = tsb
    else:
        row = Metric(user_id=user_id, date=metric_date, stress=stress, atl=atl, ctl=ctl, tsb=tsb)
        db.session.add(row)


def bulk_upsert_metrics(user_id, rows):
    """Insert or update all metric rows in a single PostgreSQL statement.

    rows — list of dicts with keys: metric_date, stress, atl, ctl, tsb.

    Replaces the N individual upsert_metric() calls done during sync.
    For a user with 2 years of history this cuts ~730 SELECT+write round-
    trips down to 1 INSERT ... ON CONFLICT DO UPDATE statement.
    """
    if not rows:
        return

    stmt = pg_insert(Metric).values([
        {
            "user_id":  user_id,
            "date":     r["metric_date"],
            "stress":   r["stress"],
            "atl":      r["atl"],
            "ctl":      r["ctl"],
            "tsb":      r["tsb"],
        }
        for r in rows
    ])
    stmt = stmt.on_conflict_do_update(
        constraint="uq_user_metric_date",
        set_={
            "stress": stmt.excluded.stress,
            "atl":    stmt.excluded.atl,
            "ctl":    stmt.excluded.ctl,
            "tsb":    stmt.excluded.tsb,
        },
    )
    db.session.execute(stmt)


def bulk_upsert_activities(user_id, rows):
    """Insert or update a batch of Strava activities in a single statement.

    rows — list of dicts with keys matching Activity columns:
      strava_activity_id, date, activity_type, distance_km, moving_time,
      avg_hr, elevation_gain, is_race.

    Replaces the per-activity upsert_activity() loop in sync_strava_data(),
    reducing N SELECT+write round-trips to 1 INSERT ... ON CONFLICT DO UPDATE.
    """
    if not rows:
        return

    stmt = pg_insert(Activity).values([
        {
            "user_id":            user_id,
            "strava_activity_id": r["strava_activity_id"],
            "date":               r["date"],
            "activity_type":      r["activity_type"],
            "distance_km":        r["distance_km"],
            "moving_time":        r["moving_time"],
            "avg_hr":             r["avg_hr"],
            "elevation_gain":     r["elevation_gain"],
            "is_race":            r["is_race"],
        }
        for r in rows
    ])
    stmt = stmt.on_conflict_do_update(
        constraint="uq_user_activity",
        set_={
            "date":          stmt.excluded.date,
            "activity_type": stmt.excluded.activity_type,
            "distance_km":   stmt.excluded.distance_km,
            "moving_time":   stmt.excluded.moving_time,
            "avg_hr":        stmt.excluded.avg_hr,
            "elevation_gain":stmt.excluded.elevation_gain,
            "is_race":       stmt.excluded.is_race,
        },
    )
    db.session.execute(stmt)


def fetch_metrics(user_id):
    return Metric.query.filter_by(user_id=user_id).order_by(Metric.date.asc()).all()


def fetch_latest_metric(user_id):
    return Metric.query.filter_by(user_id=user_id).order_by(Metric.date.desc()).first()


def create_password_reset(user_id, token, expires_at):
    row = PasswordReset(user_id=user_id, token=token, expires_at=expires_at)
    db.session.add(row)
    _commit()


def get_password_reset(token):
    return PasswordReset.query.filter_by(token=token).first()


def consume_password_reset(token):
    row = PasswordReset.query.filter_by(token=token).first()
    if row:
        db.session.delete(row)
        _commit()


def update_password(user_id, password_hash):
    user = get_user_by_id(user_id)
    if user:
        user.password_hash = password_hash
        _commit()


def save_prediction(user_id, projection_seconds):
    row = PredictionHistory(user_id=user_id, projection_seconds=projection_seconds)
    db.session.add(row)
    _commit()


def get_latest_prediction(user_id):
    return PredictionHistory.query.filter_by(user_id=user_id).order_by(PredictionHistory.created_at.desc()).first()


def fetch_recent_predictions(user_id, limit=10):
    """Return up to `limit` PredictionHistory rows, oldest-first, for sparkline display."""
    return (
        PredictionHistory.query.filter_by(user_id=user_id)
        .order_by(PredictionHistory.created_at.asc())
        .limit(limit)
        .all()
    )


def commit_all():
    _commit()

def fetch_activities_between(user_id, start_dt, end_dt):
    return (
        Activity.query.filter(Activity.user_id == user_id)
        .filter(Activity.date >= start_dt)
        .filter(Activity.date <= end_dt)
        .order_by(Activity.date.asc())
        .all()
    )


def fetch_workout_logs(user_id, start_date, end_date):
    return (
        WorkoutLog.query.filter(WorkoutLog.user_id == user_id)
        .filter(WorkoutLog.workout_date >= start_date)
        .filter(WorkoutLog.workout_date <= end_date)
        .order_by(WorkoutLog.workout_date.asc())
        .all()
    )


def delete_workout_log(user_id, workout_date):
    row = WorkoutLog.query.filter_by(user_id=user_id, workout_date=workout_date).first()
    if row:
        db.session.delete(row)
        _commit()


def upsert_workout_log(
    user_id,
    workout_date,
    workout_type,
    session_name,
    target_distance_km,
    status="planned",
    actual_distance_km=None,
    notes=None,
    source="engine",
    auto_commit=True,
):
    # Match on (user_id, workout_date, workout_type) so double-days are
    # supported — e.g. a morning run and an evening strength session on the
    # same date are treated as two distinct log entries.
    row = WorkoutLog.query.filter_by(
        user_id=user_id,
        workout_date=workout_date,
        workout_type=workout_type,
    ).first()
    if row:
        row.session_name = session_name
        row.target_distance_km = target_distance_km
        row.status = status
        row.actual_distance_km = actual_distance_km
        row.notes = notes
        row.source = source
    else:
        row = WorkoutLog(
            user_id=user_id,
            workout_date=workout_date,
            workout_type=workout_type,
            session_name=session_name,
            target_distance_km=target_distance_km,
            status=status,
            actual_distance_km=actual_distance_km,
            notes=notes,
            source=source,
        )
        db.session.add(row)

    if auto_commit:
        _commit()
    return row
