from datetime import date, datetime

import pytest

from ppi import create_app
from ppi.extensions import db
from ppi.models import Activity, CoachingPlan, Goal, Metric, PredictionHistory, RunnerProfile, StravaToken, User, WorkoutLog
from ppi.routes import (
    _build_current_week_coaching_message,
    _deterministic_long_run_progression,
    _derive_current_week_display_metrics,
    _deterministic_current_week_daily_plan,
    _deterministic_feasibility_fields,
    _deterministic_phase_label,
    _today_date_label,
    _weekly_plan_template,
)
from ppi.services.analytics_service import _long_run_progress_state, _training_phase


class TestConfig:
    TESTING = True
    SECRET_KEY = "test-secret"
    STRAVA_CLIENT_ID = "12345"
    STRAVA_CLIENT_SECRET = "secret"
    STRAVA_FETCH_PAGES = 1
    STRAVA_SCOPES = "activity:read_all,profile:read_all"
    STRAVA_REDIRECT_URI = "http://localhost:5000/auth/strava/callback"
    OPENAI_API_KEY = None
    OPENAI_MODEL = "gpt-4.1-mini"
    SQLALCHEMY_TRACK_MODIFICATIONS = False


@pytest.fixture()
def app(tmp_path):
    class Cfg(TestConfig):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'test_ppi.db'}"

    return create_app(Cfg)


@pytest.fixture()
def client(app):
    return app.test_client()


def test_dashboard_requires_login(client):
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")


def test_register_and_login_flow(client):
    register = client.post(
        "/register",
        data={"name": "Test User", "email": "u@example.com", "password": "secret12"},
        follow_redirects=False,
    )
    assert register.status_code == 302
    assert register.headers["Location"].endswith("/onboarding")

    client.get("/logout")

    login = client.post(
        "/login",
        data={"email": "u@example.com", "password": "secret12"},
        follow_redirects=False,
    )
    assert login.status_code == 302
    assert login.headers["Location"].endswith("/")


def test_onboarding_then_dashboard(client):
    from ppi.extensions import db as _db
    from ppi.models import RunnerProfile, User

    client.post(
        "/register",
        data={"name": "Runner", "email": "r@example.com", "password": "secret12"},
    )

    response = client.post(
        "/onboarding",
        data={
            "runner_name": "Runner",
            "race_name": "City Marathon",
            "race_date": "2026-12-01",
            "race_distance": "42.2",
            "goal_time": "03:45:00",
            "elevation_type": "flat",
            "current_pb": "03:59:00",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    # After onboarding, user is redirected to coach-intro (not dashboard)
    assert response.headers["Location"].endswith("/coach-intro")

    # Simulate completing coach onboarding and seed Activity rows to pass data quality gate
    from datetime import datetime, timedelta
    from ppi.models import Activity

    with client.application.app_context():
        user = User.query.filter_by(email="r@example.com").first()
        profile = RunnerProfile(
            user_id=user.id,
            consistency_level="consistent",
            race_experience="once",
            injury_status="healthy",
            training_days_per_week=5,
            long_run_day="sunday",
            strength_days_per_week=2,
            preferred_run_time="morning",
            goal_priority="hit_time",
            onboarding_completed=True,
        )
        _db.session.add(profile)

        # Seed 8 runs over 8 weeks so DataQualityReport sees sufficient data
        today = datetime.utcnow()
        for weeks_ago in range(8, 0, -1):
            act = Activity(
                user_id=user.id,
                strava_activity_id=1000 + weeks_ago,
                activity_type="run",
                date=today - timedelta(weeks=weeks_ago),
                distance_km=10.0,
                moving_time=3600.0,
                elevation_gain=0,
            )
            _db.session.add(act)
        _db.session.commit()

    dash = client.get("/", follow_redirects=False)
    assert dash.status_code == 200
    assert b"StrideIQ" in dash.data
    assert b"Weekly Plan" in dash.data


def test_oauth_login_redirect(client):
    client.post(
        "/register",
        data={"name": "Oauth", "email": "o@example.com", "password": "secret12"},
    )
    response = client.get("/connect/strava", follow_redirects=False)
    assert response.status_code == 302
    assert "https://www.strava.com/oauth/authorize" in response.headers["Location"]


def test_forgot_password_page(client):
    response = client.get("/forgot-password", follow_redirects=False)
    assert response.status_code == 200
    assert b"Reset your password" in response.data


def test_settings_page(client):
    client.post(
        "/register",
        data={"name": "Settings", "email": "s@example.com", "password": "secret12"},
    )
    settings = client.get("/settings", follow_redirects=False)
    assert settings.status_code == 200
    assert b"Settings" in settings.data


def test_training_phase_thresholds():
    assert _training_phase(7 * 20) == "base"
    assert _training_phase(7 * 15) == "build"
    assert _training_phase(7 * 8) == "peak"
    assert _training_phase(7 * 6) == "peak"
    assert _training_phase(7 * 5) == "taper"


def test_long_run_ladder_requires_true_milestone_completion():
    # Ladder is now [21, 24, 28, ...]. A run of 18.3 km has not yet cleared
    # the 21 km milestone (needs >= 21 * 0.95 = 19.95 km), so completed_step
    # is 0 and the runner is targeting 21 km next.
    runs = [{"date": date(2026, 3, 1), "distance_km": 18.3}]
    state = _long_run_progress_state(runs, date(2026, 3, 16))
    assert state["completed_step"] == 0.0
    assert state["next_step"] == 21


def test_weekly_plan_advances_long_run_to_next_ladder_step():
    # Build 3 keeps the current long-run base when the week cannot yet
    # safely support the next ladder step.
    weekly_goal = {"weekly_goal_km": 28.0, "phase": "peak", "rebuild_mode": False}
    long_run = {"longest_km": 18.3, "next_milestone_km": 12.0}
    plan = _weekly_plan_template(weekly_goal, long_run)
    long_target = float(plan[6]["target_km"])
    assert long_target == 18.3


def test_current_week_display_metrics_follow_adapted_weekly_plan():
    weekly_plan = [
        {"workout_type": "RUN", "session": "Easy Run", "planned_km": 6.0, "actual_km": 6.3, "status": "completed"},
        {"workout_type": "RUN", "session": "Aerobic Run", "day": "Tuesday", "planned_km": 5.0, "actual_km": 6.1, "status": "completed"},
        {"workout_type": "STRENGTH", "session": "Strength & Conditioning", "planned_km": 0.0, "actual_km": 0.0, "status": "completed"},
        {"workout_type": "RUN", "session": "Easy Run", "planned_km": 8.0, "actual_km": 8.0, "status": "completed"},
        {"workout_type": "STRENGTH", "session": "Strength & Conditioning", "planned_km": 0.0, "actual_km": 0.0, "status": "completed"},
        {"workout_type": "RUN", "session": "Recovery Run", "planned_km": 4.0, "actual_km": 0.0, "status": "today"},
        {"workout_type": "RUN", "session": "Long Run", "planned_km": 14.0, "actual_km": 0.0, "status": "planned"},
    ]
    metrics = _derive_current_week_display_metrics(weekly_plan, 26.0)
    assert metrics["weekly_target_km"] == 26.0
    assert metrics["actual_km"] == 20.4
    assert metrics["remaining_km"] == 5.6
    assert metrics["longest_run_km"] == 8.0
    assert metrics["planned_long_run_km"] == 14.0
    assert metrics["long_run_goal_met"] is False
    assert metrics["quality_session_name"] is None


def test_current_week_display_metrics_exposes_quality_session_when_scheduled():
    weekly_plan = [
        {"workout_type": "RUN", "session": "Easy Run", "day": "Monday", "planned_km": 6.0, "actual_km": 6.0, "status": "completed"},
        {"workout_type": "RUN", "session": "Tempo Run", "day": "Tuesday", "planned_km": 8.0, "actual_km": 0.0, "status": "planned"},
        {"workout_type": "STRENGTH", "session": "Strength & Conditioning", "planned_km": 0.0, "actual_km": 0.0, "status": "planned"},
        {"workout_type": "RUN", "session": "Long Run", "day": "Sunday", "planned_km": 16.0, "actual_km": 0.0, "status": "planned"},
    ]
    metrics = _derive_current_week_display_metrics(weekly_plan, 30.0)
    assert metrics["quality_session_name"] == "Tempo Run"
    assert metrics["quality_session_day"] == "Tuesday"
    assert metrics["quality_goal_met"] is False


def test_today_date_label_returns_string_for_valid_timezone():
    label = _today_date_label("Asia/Calcutta")
    assert isinstance(label, str)
    assert len(label) >= 8


def test_deterministic_phase_label_uses_analytics_week_fields():
    intel = {"weekly": {"display_phase": "base", "week_type": "Endurance Build"}}
    assert _deterministic_phase_label(intel) == "Base · Endurance Build"


def test_deterministic_feasibility_fields_use_current_week_metrics():
    intel = {
        "marathon_readiness_pct": 75,
        "marathon_readiness_status": "building",
        "marathon_readiness_next_step": "Complete one more long run.",
        "goal": {"days_remaining": 162},
        "weekly": {},
    }
    current_week_model = {
        "actual_km": 20.4,
        "weekly_target_km": 44.0,
        "planned_long_run_km": 15.4,
    }
    fields = _deterministic_feasibility_fields(
        intel,
        current_week_model,
        display_weekly_target_km=38,
        display_long_run_target_km=14,
    )
    assert fields["score"] == 75
    assert fields["color"] == "green"
    assert fields["label"] == "On Track"
    assert "20.4 km" in fields["text"]
    assert "38 km" in fields["text"]
    assert "14 km" in fields["text"]


def test_current_week_coaching_message_mentions_recent_long_run_when_outside_current_week():
    message = _build_current_week_coaching_message(
        26.0,
        20.4,
        8.0,
        14.0,
        False,
        False,
        None,
        None,
        18.3,
        "Sun 15 Mar",
    )
    assert "This week's longest run so far is 8.0 km." in message
    assert "18.3 km on Sun 15 Mar" in message


def test_current_week_coaching_message_uses_no_quality_session_copy_when_unscheduled():
    message = _build_current_week_coaching_message(
        36.0,
        20.4,
        8.0,
        14.0,
        False,
        False,
        None,
        None,
        18.3,
        "Sun 15 Mar",
    )
    assert "No quality session is scheduled this week." in message
    assert "still open" not in message


def test_deterministic_phase_label_suppresses_cutback_week():
    intel = {"weekly": {"display_phase": "base", "week_type": "Cutback Week"}}
    assert _deterministic_phase_label(intel) == "Base"


def test_activity_local_date_maps_utc_timestamp_into_kolkata_thursday():
    from datetime import timezone
    from ppi.routes import _activity_local_date

    # 18:45 UTC on Wed is 00:15 local on Thu in Asia/Kolkata.
    dt_value = datetime(2026, 3, 18, 18, 45, tzinfo=timezone.utc)
    assert _activity_local_date(dt_value, "Asia/Kolkata").isoformat() == "2026-03-19"


def test_deterministic_current_week_daily_plan_uses_analytics_inputs():
    intel = {
        "current_ctl": 27.2,
        "goal": {"days_remaining": 162, "distance_km": 42.195, "race_date": "2026-08-30"},
        "weekly": {
            "weekly_goal_km": 26.0,
            "phase": "base",
            "rebuild_mode": False,
            "weeks_to_race": 23.1,
            "race_distance_km": 42.195,
            "race_date": "2026-08-30",
            "prior_avg_km": 23.0,
            "training_consistency_ratio": 0.55,
            "goal_marathon_pace_sec_per_km": (3 * 3600 + 59 * 60) / 42.195,
            "high_fatigue": False,
            "moderate_fatigue": False,
            "atl_spike": False,
        },
        "long_run": {
            "longest_km": 18.3,
            "next_milestone_km": 21.0,
        },
    }
    daily_plan = _deterministic_current_week_daily_plan(intel, date(2026, 3, 16))
    assert daily_plan["monday"]["type"] == "easy"
    assert daily_plan["wednesday"]["type"] == "strength"
    assert daily_plan["sunday"]["type"] == "long"
    assert 16.5 <= daily_plan["sunday"]["km"] <= 17.0
    run_total = round(sum(float(day.get("km") or 0.0) for day in daily_plan.values()), 1)
    assert run_total == 44.0


def test_deterministic_current_week_daily_plan_preserves_long_run_for_stable_sub4_runner():
    intel = {
        "current_ctl": 35.0,
        "goal": {"days_remaining": 162, "distance_km": 42.195, "race_date": "2026-08-30"},
        "weekly": {
            "weekly_goal_km": 42.0,
            "phase": "base",
            "rebuild_mode": False,
            "weeks_to_race": 23.1,
            "race_distance_km": 42.195,
            "race_date": "2026-08-30",
            "prior_avg_km": 38.0,
            "training_consistency_ratio": 0.8,
            "goal_marathon_pace_sec_per_km": (3 * 3600 + 59 * 60) / 42.195,
            "high_fatigue": False,
            "moderate_fatigue": False,
            "atl_spike": False,
        },
        "long_run": {"longest_km": 18.3, "next_milestone_km": 21.0},
    }
    daily_plan = _deterministic_current_week_daily_plan(intel, date(2026, 3, 16))
    run_total = round(sum(float(day.get("km") or 0.0) for day in daily_plan.values()), 1)
    assert daily_plan["sunday"]["km"] == 18.3
    assert run_total == 48.2


def test_deterministic_long_run_progression_uses_whole_km_targets():
    intel = {
        "goal": {"days_remaining": 162, "distance_km": 42.195},
        "weekly": {
            "weekly_goal_km": 42.0,
            "phase": "base",
            "rebuild_mode": False,
            "weeks_to_race": 23.0,
            "race_distance_km": 42.195,
            "goal_marathon_pace_sec_per_km": (3 * 3600 + 59 * 60) / 42.195,
            "prior_avg_km": 38.0,
            "recent_avg_km": 36.0,
            "training_consistency_ratio": 0.82,
        },
        "long_run": {"longest_km": 18.3, "next_milestone_km": 21.0},
    }
    progression = _deterministic_long_run_progression(intel, date(2026, 3, 16))

    assert progression
    assert all(isinstance(item["target_km"], int) for item in progression)
    assert progression[0]["target_km"] == 20


def test_deterministic_long_run_progression_reaches_thirty_for_stable_sub4_with_runway():
    intel = {
        "goal": {"days_remaining": 162, "distance_km": 42.195},
        "weekly": {
            "weekly_goal_km": 42.0,
            "phase": "base",
            "rebuild_mode": False,
            "weeks_to_race": 23.0,
            "race_distance_km": 42.195,
            "goal_marathon_pace_sec_per_km": (3 * 3600 + 59 * 60) / 42.195,
            "prior_avg_km": 38.0,
            "recent_avg_km": 36.0,
            "training_consistency_ratio": 0.82,
        },
        "long_run": {"longest_km": 18.3, "next_milestone_km": 21.0},
    }
    progression = _deterministic_long_run_progression(intel, date(2026, 3, 16))

    assert max(item["target_km"] for item in progression) >= 30


