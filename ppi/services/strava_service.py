from datetime import datetime

import requests

from ..repositories import (
    commit_all,
    fetch_activities,
    upsert_activity,
    upsert_metric,
)
from .analytics_service import activity_stress
from .strava_oauth_service import refresh_access_token


def fetch_activities_from_strava(access_token, pages=3):
    activities = []
    for page in range(1, pages + 1):
        response = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": 50, "page": page},
            timeout=20,
        )
        response.raise_for_status()

        page_items = response.json()
        if not page_items:
            break
        activities.extend(page_items)

    return activities


def _normalize_type(activity):
    sport_type = str(activity.get("sport_type") or "").lower()
    primary = str(activity.get("type") or "").lower()

    if sport_type in {"run", "walk", "ride", "swim", "yoga"}:
        return sport_type
    if sport_type in {"weighttraining", "workout"}:
        return "strength"

    if primary in {"run", "walk", "ride", "swim", "yoga"}:
        return primary
    if primary in {"weighttraining", "workout"}:
        return "strength"

    return primary or "cross_training"


def _is_race_event(activity):
    name = str(activity.get("name") or "").lower()
    workout_type = activity.get("workout_type")
    return workout_type == 1 or "race" in name


def _load_metrics(user_id):
    all_activities = fetch_activities(user_id)
    if not all_activities:
        return

    by_day = {}
    for a in all_activities:
        day = a.date.date()
        stress = activity_stress(a.activity_type, a.moving_time, a.avg_hr)
        by_day[day] = by_day.get(day, 0.0) + stress

    atl = 0.0
    ctl = 0.0
    ordered_days = sorted(by_day.items(), key=lambda x: x[0])

    for metric_day, stress in ordered_days:
        atl = atl + (stress - atl) * (1.0 / 7.0)
        ctl = ctl + (stress - ctl) * (1.0 / 42.0)
        tsb = ctl - atl
        upsert_metric(
            user_id=user_id,
            metric_date=metric_day,
            stress=round(stress, 2),
            atl=round(atl, 2),
            ctl=round(ctl, 2),
            tsb=round(tsb, 2),
        )


def sync_strava_data(user_id, pages=3):
    access_token = refresh_access_token(user_id)
    if not access_token:
        return {"status": "skipped", "reason": "not_connected", "new_activities": 0}

    fetched = fetch_activities_from_strava(access_token, pages=pages)
    if not fetched:
        return {"status": "ok", "reason": "up_to_date", "new_activities": 0}

    new_count = 0
    for activity in fetched:
        start_date = activity.get("start_date")
        if not start_date:
            continue

        date_utc = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
        strava_id = activity.get("id")
        if not strava_id:
            continue

        upsert_activity(
            user_id=user_id,
            strava_activity_id=int(strava_id),
            date_utc=date_utc,
            activity_type=_normalize_type(activity),
            distance_km=(activity.get("distance") or 0) / 1000.0,
            moving_time=float(activity.get("moving_time") or 0.0),
            avg_hr=activity.get("average_heartrate"),
            elevation_gain=float(activity.get("total_elevation_gain") or 0.0),
            is_race=_is_race_event(activity),
        )
        new_count += 1

    _load_metrics(user_id)
    commit_all()

    return {"status": "ok", "reason": "synced", "new_activities": new_count}

