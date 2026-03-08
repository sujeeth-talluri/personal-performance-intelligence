import secrets
import time

import requests
from flask import current_app

from ..repositories import get_user_by_id, save_strava_tokens


def generate_oauth_state():
    return secrets.token_urlsafe(24)


def get_authorize_url(state):
    cfg = current_app.config
    params = {
        "client_id": cfg["STRAVA_CLIENT_ID"],
        "response_type": "code",
        "redirect_uri": cfg["STRAVA_REDIRECT_URI"],
        "approval_prompt": "auto",
        "scope": cfg["STRAVA_SCOPES"],
        "state": state,
    }
    query = "&".join(f"{key}={requests.utils.quote(str(value))}" for key, value in params.items())
    return f"https://www.strava.com/oauth/authorize?{query}"


def exchange_code_for_token(code):
    cfg = current_app.config
    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": cfg["STRAVA_CLIENT_ID"],
            "client_secret": cfg["STRAVA_CLIENT_SECRET"],
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def link_oauth_identity(user_id, oauth_payload):
    athlete_data = oauth_payload.get("athlete") or {}
    save_strava_tokens(
        user_id=user_id,
        strava_athlete_id=athlete_data.get("id"),
        access_token=oauth_payload["access_token"],
        refresh_token=oauth_payload["refresh_token"],
        expires_at=oauth_payload["expires_at"],
    )


def refresh_access_token(user_id):
    user = get_user_by_id(user_id)
    if not user:
        return None

    now = int(time.time())
    if user["strava_access_token"] and user["strava_access_expires_at"] and user["strava_access_expires_at"] > now + 60:
        return user["strava_access_token"]

    refresh_token = user["strava_refresh_token"]
    if not refresh_token:
        return None

    cfg = current_app.config
    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": cfg["STRAVA_CLIENT_ID"],
            "client_secret": cfg["STRAVA_CLIENT_SECRET"],
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()

    save_strava_tokens(
        user_id=user_id,
        strava_athlete_id=payload.get("athlete", {}).get("id") or user["strava_athlete_id"],
        access_token=payload["access_token"],
        refresh_token=payload["refresh_token"],
        expires_at=payload["expires_at"],
    )
    return payload["access_token"]
