import os


def _normalize_database_url(url):
    if not url:
        return None
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")

    DATABASE_URL = _normalize_database_url(os.getenv("DATABASE_URL"))
    SQLALCHEMY_DATABASE_URI = DATABASE_URL
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    STRAVA_CLIENT_ID = os.getenv("CLIENT_ID")
    STRAVA_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    STRAVA_FETCH_PAGES = int(os.getenv("STRAVA_FETCH_PAGES", "3"))
    STRAVA_SCOPES = os.getenv("STRAVA_SCOPES", "activity:read_all,profile:read_all")
    STRAVA_REDIRECT_URI = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:5000/auth/strava/callback")

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SMTP_FROM = os.getenv("SMTP_FROM", "noreply@strideiq.app")
