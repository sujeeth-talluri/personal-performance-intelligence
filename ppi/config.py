import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def _normalize_database_url(url):
    if not url:
        return None
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def _is_supabase_url(url: str) -> bool:
    """Detect Supabase-hosted PostgreSQL by hostname pattern."""
    try:
        host = urlsplit(url).hostname or ""
        return "supabase.co" in host or "supabase.com" in host
    except Exception:
        return False


def _database_engine_options(url):
    if not url:
        return {}

    options = {"pool_pre_ping": True}
    if not url.startswith("postgresql://"):
        return options

    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("connect_timeout", "10")

    # Supabase requires SSL and has tighter connection limits on free tier.
    # Pool: 5 persistent + 2 overflow = 7 max per worker process.
    # With 2 Gunicorn workers that is 14 connections — safely under the ~20 limit.
    pool_size = 5
    max_overflow = 2
    pool_timeout = 10

    connect_args = {"connect_timeout": int(query["connect_timeout"])}
    if _is_supabase_url(url):
        # Force SSL — Supabase rejects or downgrades unencrypted connections.
        connect_args["sslmode"] = "require"

    normalized_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    return {
        "pool_pre_ping": True,
        "pool_recycle": 300,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_timeout": pool_timeout,
        "connect_args": connect_args,
        "_normalized_url": normalized_url,
    }


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY")

    DATABASE_URL = _normalize_database_url(os.getenv("DATABASE_URL"))
    _ENGINE_OPTIONS = _database_engine_options(DATABASE_URL)
    SQLALCHEMY_DATABASE_URI = _ENGINE_OPTIONS.pop("_normalized_url", DATABASE_URL)
    SQLALCHEMY_ENGINE_OPTIONS = _ENGINE_OPTIONS
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    STRAVA_CLIENT_ID = os.getenv("CLIENT_ID")
    STRAVA_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    STRAVA_FETCH_PAGES = int(os.getenv("STRAVA_FETCH_PAGES", "3"))
    STRAVA_SYNC_COOLDOWN_MIN = int(os.getenv("STRAVA_SYNC_COOLDOWN_MIN", "15"))
    STRAVA_SCOPES = os.getenv("STRAVA_SCOPES", "activity:read_all,profile:read_all")
    STRAVA_REDIRECT_URI = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:5000/auth/strava/callback")

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5")

    SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
    SESSION_COOKIE_HTTPONLY = os.getenv("SESSION_COOKIE_HTTPONLY", "true").lower() == "true"
    SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")

    # CSRF — Flask-WTF
    WTF_CSRF_TIME_LIMIT = 3600  # 1 hour token expiry
    WTF_CSRF_SSL_STRICT = False  # allow http in local dev; tightened by SESSION_COOKIE_SECURE in prod
    ALLOW_ADMIN_RESET = os.getenv("ALLOW_ADMIN_RESET", "0").lower() in {"1", "true", "yes", "on"}
    ADMIN_RESET_EMAIL = os.getenv("ADMIN_RESET_EMAIL")

    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SMTP_FROM = os.getenv("SMTP_FROM", "noreply@strideiq.app")


