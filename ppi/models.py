from datetime import datetime, timezone

from .extensions import db


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(255), nullable=False, unique=True, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=_utcnow_naive, nullable=False)

    goals = db.relationship("Goal", backref="user", lazy=True, cascade="all, delete-orphan")


class Goal(db.Model):
    __tablename__ = "goals"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    race_name = db.Column(db.String(180), nullable=False)
    race_distance = db.Column(db.Float, nullable=False)
    race_date = db.Column(db.Date, nullable=False)
    goal_time = db.Column(db.String(16), nullable=False)
    elevation_type = db.Column(db.String(24), nullable=False)
    personal_best = db.Column(db.String(16), nullable=True)
    pb_5k  = db.Column(db.String(16), nullable=True)
    pb_10k = db.Column(db.String(16), nullable=True)
    pb_hm  = db.Column(db.String(16), nullable=True)


class StravaToken(db.Model):
    __tablename__ = "strava_tokens"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True, index=True)
    athlete_id = db.Column(db.BigInteger, nullable=True, index=True)
    access_token = db.Column(db.Text, nullable=False)
    refresh_token = db.Column(db.Text, nullable=False)
    expires_at = db.Column(db.Integer, nullable=False)


class Activity(db.Model):
    __tablename__ = "activities"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    strava_activity_id = db.Column(db.BigInteger, nullable=False, index=True)
    date = db.Column(db.DateTime, nullable=False)
    activity_type = db.Column(db.String(64), nullable=False)
    distance_km = db.Column(db.Float, nullable=False)
    moving_time = db.Column(db.Float, nullable=False)
    avg_hr = db.Column(db.Float, nullable=True)
    elevation_gain = db.Column(db.Float, nullable=True)
    is_race = db.Column(db.Boolean, default=False, nullable=False)

    __table_args__ = (db.UniqueConstraint("user_id", "strava_activity_id", name="uq_user_activity"),)


class Metric(db.Model):
    __tablename__ = "metrics"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    date = db.Column(db.Date, nullable=False, index=True)
    stress = db.Column(db.Float, nullable=False)
    atl = db.Column(db.Float, nullable=False)
    ctl = db.Column(db.Float, nullable=False)
    tsb = db.Column(db.Float, nullable=False)

    __table_args__ = (db.UniqueConstraint("user_id", "date", name="uq_user_metric_date"),)


class PasswordReset(db.Model):
    __tablename__ = "password_resets"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    token = db.Column(db.String(255), nullable=False, unique=True, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)


class PredictionHistory(db.Model):
    __tablename__ = "prediction_history"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=_utcnow_naive, nullable=False)
    projection_seconds = db.Column(db.Float, nullable=False)

class WorkoutLog(db.Model):
    __tablename__ = "workout_logs"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    workout_date = db.Column(db.Date, nullable=False, index=True)
    workout_type = db.Column(db.String(16), nullable=False)  # RUN | STRENGTH | REST
    session_name = db.Column(db.String(64), nullable=False)
    target_distance_km = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(16), nullable=False, default="planned")  # planned | completed | skipped | overperformed
    actual_distance_km = db.Column(db.Float, nullable=True)
    notes = db.Column(db.String(255), nullable=True)
    source = db.Column(db.String(24), nullable=False, default="engine")

    __table_args__ = (db.UniqueConstraint("user_id", "workout_date", name="uq_user_workout_date"),)
