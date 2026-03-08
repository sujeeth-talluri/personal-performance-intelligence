import time
from datetime import datetime

import requests

from ..repositories import fetch_all_metrics, fetch_latest_metric, save_metrics
from .analytics_service import calculate_readiness, compute_stress, update_training_load
from .strava_oauth_service import refresh_access_token


def fetch_activities(access_token, pages=3, after_timestamp=None):
    activities = []
    for page in range(1, pages + 1):
        params = {"per_page": 30, "page": page}
        if after_timestamp:
            params["after"] = after_timestamp

        response = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            timeout=20,
        )
        response.raise_for_status()

        page_items = response.json()
        if not page_items:
            break
        activities.extend(page_items)

    return activities


def _starting_load(user_id):
    rows = fetch_all_metrics(user_id)
    if not rows:
        return 0.0, 0.0

    latest = rows[-1]
    return float(latest["atl"] or 0), float(latest["ctl"] or 0)


def sync_strava_data(user_id, pages=3):
    access_token = refresh_access_token(user_id)
    if not access_token:
        return {"status": "skipped", "reason": "not_connected", "new_activities": 0}

    latest = fetch_latest_metric(user_id)
    after_timestamp = None
    if latest:
        latest_dt = datetime.strptime(latest["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        after_timestamp = int(time.mktime(latest_dt.timetuple())) + 1

    fetched = fetch_activities(access_token, pages=pages, after_timestamp=after_timestamp)
    if not fetched:
        return {"status": "ok", "reason": "up_to_date", "new_activities": 0}

    fetched.sort(key=lambda item: item["start_date"])

    stress_by_date = {}
    activity_enriched = []

    for activity in fetched:
        run_date = datetime.strptime(activity["start_date"], "%Y-%m-%dT%H:%M:%SZ").date()
        stress = compute_stress(activity)
        stress_by_date[run_date] = stress_by_date.get(run_date, 0.0) + stress
        activity_enriched.append((activity, run_date, stress))

    starting_atl, starting_ctl = _starting_load(user_id)
    load_by_date = update_training_load(stress_by_date, starting_atl, starting_ctl)

    for activity, run_date, stress in activity_enriched:
        load = load_by_date[run_date]
        readiness = calculate_readiness(load["tsb"])

        save_metrics(
            user_id=user_id,
            activity_id=activity["id"],
            timestamp=activity["start_date"],
            distance_km=(activity.get("distance") or 0) / 1000,
            moving_time_sec=float(activity.get("moving_time") or 0),
            avg_hr=activity.get("average_heartrate"),
            elevation_gain_m=float(activity.get("total_elevation_gain") or 0),
            stress=stress,
            atl=load["atl"],
            ctl=load["ctl"],
            tsb=load["tsb"],
            readiness=readiness,
        )

    return {"status": "ok", "reason": "synced", "new_activities": len(fetched)}
