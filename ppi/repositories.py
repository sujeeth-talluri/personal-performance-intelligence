from datetime import date, datetime

from .extensions import db
from .models import Activity, Goal, Metric, PasswordReset, PredictionHistory, StravaToken, User


def create_user(name, email, password_hash):
    user = User(name=name.strip(), email=email.strip().lower(), password_hash=password_hash)
    db.session.add(user)
    db.session.commit()
    return user.id


def get_user_by_email(email):
    return User.query.filter_by(email=email.strip().lower()).first()


def get_user_by_id(user_id):
    return db.session.get(User, user_id)


def update_user_name(user_id, name):
    user = get_user_by_id(user_id)
    if user:
        user.name = name.strip()
        db.session.commit()


def save_goal(user_id, race_name, race_distance, goal_time, race_date, elevation_type, personal_best):
    goal = Goal.query.filter_by(user_id=user_id).order_by(Goal.id.desc()).first()
    race_date_obj = datetime.strptime(race_date, "%Y-%m-%d").date()

    if goal:
        goal.race_name = race_name
        goal.race_distance = race_distance
        goal.goal_time = goal_time
        goal.race_date = race_date_obj
        goal.elevation_type = elevation_type
        goal.personal_best = personal_best
    else:
        goal = Goal(
            user_id=user_id,
            race_name=race_name,
            race_distance=race_distance,
            goal_time=goal_time,
            race_date=race_date_obj,
            elevation_type=elevation_type,
            personal_best=personal_best,
        )
        db.session.add(goal)

    db.session.commit()


def get_goal(user_id):
    return Goal.query.filter_by(user_id=user_id).order_by(Goal.id.desc()).first()


def save_strava_tokens(user_id, athlete_id, access_token, refresh_token, expires_at):
    token = StravaToken.query.filter_by(user_id=user_id).first()
    if token:
        token.athlete_id = athlete_id
        token.access_token = access_token
        token.refresh_token = refresh_token
        token.expires_at = int(expires_at)
    else:
        token = StravaToken(
            user_id=user_id,
            athlete_id=athlete_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=int(expires_at),
        )
        db.session.add(token)
    db.session.commit()


def get_strava_token(user_id):
    return StravaToken.query.filter_by(user_id=user_id).first()


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


def fetch_metrics(user_id):
    return Metric.query.filter_by(user_id=user_id).order_by(Metric.date.asc()).all()


def fetch_latest_metric(user_id):
    return Metric.query.filter_by(user_id=user_id).order_by(Metric.date.desc()).first()


def create_password_reset(user_id, token, expires_at):
    row = PasswordReset(user_id=user_id, token=token, expires_at=expires_at)
    db.session.add(row)
    db.session.commit()


def get_password_reset(token):
    return PasswordReset.query.filter_by(token=token).first()


def consume_password_reset(token):
    row = PasswordReset.query.filter_by(token=token).first()
    if row:
        db.session.delete(row)
        db.session.commit()


def update_password(user_id, password_hash):
    user = get_user_by_id(user_id)
    if user:
        user.password_hash = password_hash
        db.session.commit()


def save_prediction(user_id, projection_seconds):
    row = PredictionHistory(user_id=user_id, projection_seconds=projection_seconds)
    db.session.add(row)
    db.session.commit()


def get_latest_prediction(user_id):
    return PredictionHistory.query.filter_by(user_id=user_id).order_by(PredictionHistory.created_at.desc()).first()


def commit_all():
    db.session.commit()

