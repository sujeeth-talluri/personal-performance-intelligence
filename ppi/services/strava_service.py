from datetime import datetime

import requests

from ..repositories import (
    bulk_upsert_activities,
    bulk_upsert_metrics,
    commit_all,
    fetch_activities,
    get_goal,
)
from .load_engine import _ATL_DECAY, _ATL_GAIN, _CTL_DECAY, _CTL_GAIN, running_stress_score
from .strava_oauth_service import refresh_access_token


def _hms_to_sec(hms: str) -> float:
    """Parse 'H:MM:SS' goal time string to total seconds."""
    try:
        parts = hms.strip().split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, AttributeError):
        pass
    return 0.0


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
    """Recompute and persist ATL/CTL/TSB for all stored activities.

    Uses load_engine.running_stress_score (duration × IF²) and the correct
    exponential decay constants so the stored Metric rows match what the
    dashboard computes live.

    All rows are written in a single bulk INSERT ... ON CONFLICT DO UPDATE
    statement instead of N individual round-trips.
    """
    all_activities = fetch_activities(user_id)
    if not all_activities:
        return

    # Derive marathon pace from the user's goal so intensity zones are correct.
    goal = get_goal(user_id)
    if goal and goal.goal_time and goal.race_distance:
        goal_seconds = _hms_to_sec(goal.goal_time)
        marathon_pace = goal_seconds / float(goal.race_distance) if goal_seconds > 0 else 360.0
    else:
        marathon_pace = 360.0  # default 6:00/km when no goal is set

    by_day: dict = {}
    for a in all_activities:
        day = a.date.date() if hasattr(a.date, "date") else a.date
        distance_km = float(a.distance_km or 0.0)
        moving_time_sec = float(a.moving_time or 0.0)
        pace = (moving_time_sec / distance_km) if distance_km > 0 else None
        activity_dict = {
            "type": a.activity_type or "run",
            "distance_km": distance_km,
            "moving_time_sec": moving_time_sec,
            "elevation_gain": float(a.elevation_gain or 0.0),
            "avg_hr": a.avg_hr,
            "pace_sec_per_km": pace,
        }
        stress = running_stress_score(activity_dict, marathon_pace)
        by_day[day] = by_day.get(day, 0.0) + stress

    # Accumulate all computed rows, then write in one bulk statement.
    atl = 0.0
    ctl = 0.0
    metric_rows = []
    for metric_day, stress in sorted(by_day.items()):
        atl = atl * _ATL_DECAY + stress * _ATL_GAIN
        ctl = ctl * _CTL_DECAY + stress * _CTL_GAIN
        tsb = ctl - atl
        metric_rows.append({
            "metric_date": metric_day,
            "stress":      round(stress, 2),
            "atl":         round(atl, 2),
            "ctl":         round(ctl, 2),
            "tsb":         round(tsb, 2),
        })

    bulk_upsert_metrics(user_id, metric_rows)


def sync_strava_data(user_id, pages=3):
    access_token = refresh_access_token(user_id)
    if not access_token:
        return {"status": "skipped", "reason": "not_connected", "new_activities": 0}

    fetched = fetch_activities_from_strava(access_token, pages=pages)
    if not fetched:
        return {"status": "ok", "reason": "up_to_date", "new_activities": 0}

    # Build the full batch — filter invalid rows in Python, then write once.
    activity_rows = []
    for activity in fetched:
        start_date = activity.get("start_date")
        strava_id  = activity.get("id")
        if not start_date or not strava_id:
            continue
        activity_rows.append({
            "strava_activity_id": int(strava_id),
            "date":               datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ"),
            "activity_type":      _normalize_type(activity),
            "distance_km":        (activity.get("distance") or 0) / 1000.0,
            "moving_time":        float(activity.get("moving_time") or 0.0),
            "avg_hr":             activity.get("average_heartrate"),
            "elevation_gain":     float(activity.get("total_elevation_gain") or 0.0),
            "is_race":            _is_race_event(activity),
        })

    bulk_upsert_activities(user_id, activity_rows)
    _load_metrics(user_id)
    commit_all()

    return {"status": "ok", "reason": "synced", "new_activities": len(activity_rows)}

