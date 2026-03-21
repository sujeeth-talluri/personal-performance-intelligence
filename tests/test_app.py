from datetime import date, datetime

import pytest

from ppi import create_app
from ppi.extensions import db
from ppi.models import Activity, CoachingPlan, Goal, Metric, PredictionHistory, RunnerProfile, StravaToken, User, WorkoutLog
from ppi.routes import _weekly_plan_template
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
    ALLOW_ADMIN_RESET = True


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
    # CTL-based template does not cap long run by weekly volume —
    # it always advances to the next ladder milestone.
    weekly_goal = {"weekly_goal_km": 28.0, "phase": "peak", "rebuild_mode": False}
    long_run = {"longest_km": 18.3, "next_milestone_km": 12.0}
    plan = _weekly_plan_template(weekly_goal, long_run)
    long_target = float(plan[6]["target_km"])
    assert long_target >= 21.0  # next step after 18km in ladder


def test_admin_reset_training_data_clears_training_tables_only(client, app):
    client.post(
        "/register",
        data={"name": "Reset User", "email": "reset@example.com", "password": "secret12"},
        follow_redirects=False,
    )

    with app.app_context():
        user = User.query.filter_by(email="reset@example.com").first()
        db.session.add(Goal(
            user_id=user.id,
            race_name="Reset Marathon",
            race_distance=42.2,
            race_date=date(2026, 12, 1),
            goal_time="03:45:00",
            elevation_type="flat",
        ))
        db.session.add(RunnerProfile(
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
        ))
        db.session.add(StravaToken(
            user_id=user.id,
            athlete_id=123456,
            access_token="a",
            refresh_token="r",
            expires_at=9999999999,
        ))
        db.session.add(Activity(
            user_id=user.id,
            strava_activity_id=1,
            activity_type="run",
            date=datetime(2026, 3, 20, 6, 0, 0),
            distance_km=10.0,
            moving_time=3600.0,
            elevation_gain=0,
        ))
        db.session.add(Metric(user_id=user.id, date=date(2026, 3, 20), stress=50.0, atl=20.0, ctl=25.0, tsb=5.0))
        db.session.add(WorkoutLog(
            user_id=user.id,
            workout_date=date(2026, 3, 20),
            workout_type="RUN",
            session_name="Easy Run",
            target_distance_km=8.0,
            status="completed",
            actual_distance_km=10.0,
            source="engine",
        ))
        db.session.add(PredictionHistory(user_id=user.id, projection_seconds=14400.0))
        db.session.add(CoachingPlan(
            user_id=user.id,
            generated_at=datetime(2026, 3, 20, 6, 0, 0),
            plan_json="{}",
            context_json="{}",
            weekly_target_km=40.0,
            long_run_km=18.0,
        ))
        db.session.commit()

    get_page = client.get("/admin/reset-training-data", follow_redirects=False)
    assert get_page.status_code == 200
    assert b"RESET TRAINING DATA" in get_page.data

    with client.session_transaction() as sess:
        nonce = sess["admin_reset_nonce"]

    post_page = client.post(
        "/admin/reset-training-data",
        data={"nonce": nonce, "confirm_text": "RESET TRAINING DATA"},
        follow_redirects=False,
    )
    assert post_page.status_code == 200
    assert b"Training data reset complete" in post_page.data

    with app.app_context():
        user = User.query.filter_by(email="reset@example.com").first()
        assert Activity.query.filter_by(user_id=user.id).count() == 0
        assert Metric.query.filter_by(user_id=user.id).count() == 0
        assert WorkoutLog.query.filter_by(user_id=user.id).count() == 0
        assert PredictionHistory.query.filter_by(user_id=user.id).count() == 0
        assert CoachingPlan.query.filter_by(user_id=user.id).count() == 0
        assert Goal.query.filter_by(user_id=user.id).count() == 1
        assert RunnerProfile.query.filter_by(user_id=user.id).count() == 1
        assert StravaToken.query.filter_by(user_id=user.id).count() == 1
