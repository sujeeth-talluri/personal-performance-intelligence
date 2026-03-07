import pytest

from ppi import create_app
from ppi.repositories import create_athlete, insert_goal


class TestConfig:
    TESTING = True
    SECRET_KEY = "test-secret"
    DATABASE_PATH = ""
    STRAVA_CLIENT_ID = None
    STRAVA_CLIENT_SECRET = None
    STRAVA_REFRESH_TOKEN = None
    STRAVA_FETCH_PAGES = 1
    STRAVA_SCOPES = "activity:read_all,profile:read_all"
    STRAVA_REDIRECT_URI = "http://localhost:5000/auth/strava/callback"
    OPENAI_API_KEY = None
    OPENAI_MODEL = "gpt-4.1-mini"


@pytest.fixture()
def app(tmp_path):
    class Cfg(TestConfig):
        DATABASE_PATH = str(tmp_path / "test_ppi.db")

    app = create_app(Cfg)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def test_dashboard_redirects_to_setup_without_goal(client):
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].startswith("/setup")


def test_add_athlete_redirects_to_setup(client):
    response = client.post("/athletes", data={"name": "Runner One"}, follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].startswith("/setup?athlete_id=")


def test_setup_then_dashboard_renders(client):
    with client.application.app_context():
        athlete_id = create_athlete("Runner Two")

    setup_response = client.post(
        f"/setup?athlete_id={athlete_id}",
        data={
            "race_name": "City Marathon",
            "race_date": "2026-12-01",
            "distance": "42.2",
            "goal_time": "03:45:00",
            "elevation": "flat",
        },
        follow_redirects=False,
    )
    assert setup_response.status_code == 302

    dashboard_response = client.get(f"/?athlete_id={athlete_id}", follow_redirects=False)
    assert dashboard_response.status_code == 200
    assert b"Performance Intelligence Platform" in dashboard_response.data


def test_oauth_login_redirect_contains_strava_authorize(app, client):
    app.config["STRAVA_CLIENT_ID"] = "12345"
    app.config["STRAVA_REDIRECT_URI"] = "http://localhost:5000/auth/strava/callback"

    response = client.get("/auth/strava/login", follow_redirects=False)
    assert response.status_code == 302
    assert "https://www.strava.com/oauth/authorize" in response.headers["Location"]


def test_dashboard_with_existing_goal_and_no_metrics_still_renders(client):
    with client.application.app_context():
        athlete_id = create_athlete("Runner Three")
        insert_goal(
            athlete_id=athlete_id,
            event_name="Half Marathon",
            distance_km=21.1,
            goal_time="01:45:00",
            race_date="2026-09-10",
            elevation_type="moderate",
        )

    response = client.get(f"/?athlete_id={athlete_id}", follow_redirects=False)
    assert response.status_code == 200
    assert b"No metrics available yet" in response.data
