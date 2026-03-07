import os


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
    DATABASE_PATH = os.getenv("PPI_DB_PATH", "ppi.db")

    STRAVA_CLIENT_ID = os.getenv("CLIENT_ID")
    STRAVA_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    STRAVA_REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
    STRAVA_FETCH_PAGES = int(os.getenv("STRAVA_FETCH_PAGES", "3"))
    STRAVA_SCOPES = os.getenv("STRAVA_SCOPES", "activity:read_all,profile:read_all")
    STRAVA_REDIRECT_URI = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:5000/auth/strava/callback")

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
