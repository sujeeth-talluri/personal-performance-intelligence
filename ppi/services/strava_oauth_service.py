import secrets
import time

import requests
from flask import current_app

from ..repositories import get_strava_account, save_strava_account


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


def refresh_access_token(athlete_id):
    cfg = current_app.config
    account = get_strava_account(athlete_id)
    if not account:
        fallback_refresh = cfg.get("STRAVA_REFRESH_TOKEN")
        if not fallback_refresh:
            return None

        response = requests.post(
            "https://www.strava.com/oauth/token",
            data={
                "client_id": cfg["STRAVA_CLIENT_ID"],
                "client_secret": cfg["STRAVA_CLIENT_SECRET"],
                "grant_type": "refresh_token",
                "refresh_token": fallback_refresh,
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        save_strava_account(
            athlete_id=athlete_id,
            strava_athlete_id=payload.get("athlete", {}).get("id"),
            refresh_token=payload["refresh_token"],
            access_token=payload["access_token"],
            access_expires_at=payload["expires_at"],
        )
        return payload["access_token"]

    now = int(time.time())
    if account["access_token"] and account["access_expires_at"] and account["access_expires_at"] > now + 60:
        return account["access_token"]

    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": cfg["STRAVA_CLIENT_ID"],
            "client_secret": cfg["STRAVA_CLIENT_SECRET"],
            "grant_type": "refresh_token",
            "refresh_token": account["refresh_token"],
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()

    save_strava_account(
        athlete_id=athlete_id,
        strava_athlete_id=payload.get("athlete", {}).get("id") or account["strava_athlete_id"],
        refresh_token=payload["refresh_token"],
        access_token=payload["access_token"],
        access_expires_at=payload["expires_at"],
    )
    return payload["access_token"]


def link_oauth_identity(athlete_id, oauth_payload):
    athlete_data = oauth_payload.get("athlete") or {}
    save_strava_account(
        athlete_id=athlete_id,
        strava_athlete_id=athlete_data.get("id"),
        refresh_token=oauth_payload["refresh_token"],
        access_token=oauth_payload["access_token"],
        access_expires_at=oauth_payload["expires_at"],
    )
