import pytest

from ppi import create_app


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
    assert response.headers["Location"].endswith("/")

    dash = client.get("/", follow_redirects=False)
    assert dash.status_code == 200
    assert b"Current Capability (Flat Course)" in dash.data


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

