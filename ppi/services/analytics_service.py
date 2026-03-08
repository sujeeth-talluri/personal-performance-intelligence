from datetime import date, datetime, timedelta, timezone

from ..repositories import (
    fetch_activities,
    fetch_latest_metric,
    fetch_metrics,
    fetch_recent_activities,
    get_goal,
    get_latest_prediction,
    save_prediction,
)

MARATHON_KM = 42.195
THRESHOLD_HR = 168
STRESS_TYPE_FACTOR = {
    "run": 1.0,
    "walk": 0.55,
    "ride": 0.85,
    "swim": 0.80,
    "strength": 0.70,
    "yoga": 0.35,
}


def _utc_today():
    return datetime.now(timezone.utc).date()


def _hms_to_seconds(hms):
    h, m, s = map(int, hms.split(":"))
    return h * 3600 + m * 60 + s


def _fmt_hms(total_seconds):
    sec = max(0, int(total_seconds))
    return f"{sec // 3600}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


def _fmt_gap(total_seconds):
    sign = "+" if total_seconds > 0 else "-"
    sec = abs(int(total_seconds))
    return f"{sign}{sec // 3600}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


def activity_stress(activity_type, moving_time_seconds, avg_hr):
    duration_min = (moving_time_seconds or 0) / 60.0
    if duration_min <= 0:
        return 0.0

    t = (activity_type or "run").lower()
    factor = STRESS_TYPE_FACTOR.get(t, 0.70)
    hr_factor = (avg_hr / THRESHOLD_HR) if avg_hr else 0.75
    return round(duration_min * hr_factor * factor, 2)


def _elevation_factor(elevation_type):
    factors = {
        "flat": 1.00,
        "moderate": 1.02,
        "hilly": 1.05,
        "mountain": 1.10,
    }
    return max(1.0, factors.get((elevation_type or "moderate").lower(), 1.02))


def _fatigue_multiplier(weekly_km):
    if weekly_km < 40:
        return 1.15
    if weekly_km < 60:
        return 1.12
    if weekly_km < 80:
        return 1.09
    if weekly_km < 100:
        return 1.06
    return 1.04


def _status_color(status):
    s = (status or "").lower()
    if "insufficient" in s:
        return "grey"
    if any(x in s for x in ["ideal", "strong", "excellent", "elite"]):
        return "green"
    if any(x in s for x in ["aggressive", "moderate", "caution"]):
        return "orange"
    if any(x in s for x in ["risk", "weak", "too low"]):
        return "red"
    return "grey"


def build_goal_context(goal):
    if not goal:
        return None

    goal_seconds = _hms_to_seconds(goal.goal_time)
    race_date = goal.race_date
    days_remaining = (race_date - date.today()).days
    pace_sec = goal_seconds / float(goal.race_distance)

    return {
        "race_name": goal.race_name,
        "distance_km": float(goal.race_distance),
        "goal_time": goal.goal_time,
        "goal_seconds": goal_seconds,
        "race_date": race_date.isoformat(),
        "days_remaining": days_remaining,
        "target_pace": f"{int(pace_sec // 60)}:{int(pace_sec % 60):02d} / km",
        "elevation_type": goal.elevation_type,
        "elevation_factor": _elevation_factor(goal.elevation_type),
        "projection_label": f"{goal.race_name} Projection ({goal.elevation_type.title()} Course)",
    }


def _activity_to_raw(a):
    distance = float(a.distance_km or 0)
    moving_time = float(a.moving_time or 0)
    pace_sec_per_km = (moving_time / distance) if distance > 0 and moving_time > 0 else None
    return {
        "id": a.strava_activity_id,
        "date": a.date.date(),
        "type": (a.activity_type or "").lower(),
        "is_race": bool(a.is_race),
        "distance_km": distance,
        "moving_time_sec": moving_time,
        "pace_sec_per_km": pace_sec_per_km,
        "avg_hr": float(a.avg_hr) if a.avg_hr else None,
    }


def _raw_activity_layer(user_id):
    return [_activity_to_raw(a) for a in fetch_activities(user_id)]


def _weekly_buckets(runs, weeks):
    today = _utc_today()
    start_week = today - timedelta(days=today.weekday())
    weekly = {start_week - timedelta(days=7 * i): 0.0 for i in range(weeks)}
    for r in runs:
        wk = r["date"] - timedelta(days=r["date"].weekday())
        if wk in weekly:
            weekly[wk] += r["distance_km"]
    return weekly, start_week


def _baseline_weekly_goal(goal_seconds):
    if goal_seconds <= 3 * 3600:
        return 90.0
    if goal_seconds <= 3 * 3600 + 30 * 60:
        return 75.0
    if goal_seconds <= 4 * 3600:
        return 60.0
    return 45.0


def _metrics_layer(user_id, goal_context):
    raw = _raw_activity_layer(user_id)
    today = _utc_today()
    recent_since = today - timedelta(days=56)
    durability_since = today - timedelta(days=112)

    run_training = [
        r
        for r in raw
        if r["type"] == "run"
        and not r["is_race"]
        and r["distance_km"] <= 42.2
        and r["distance_km"] > 0
        and r["moving_time_sec"] > 0
    ]

    runs_recent = [r for r in run_training if r["date"] >= recent_since]
    runs_16w = [r for r in run_training if r["date"] >= durability_since]

    medium_runs = [r for r in runs_recent if 8 <= r["distance_km"] <= 12]
    long_runs = [r for r in runs_recent if 18 <= r["distance_km"] <= 30]

    weekly, this_week = _weekly_buckets(runs_recent, 8)
    avg_weekly_km = round(sum(weekly.values()) / len(weekly), 1) if weekly else 0.0
    current_week_km = round(weekly.get(this_week, 0.0), 1)
    consistent_weeks = len([v for v in weekly.values() if v >= 25])

    longest_recent = max(runs_recent, key=lambda x: x["distance_km"], default=None)
    longest_16w = max(runs_16w, key=lambda x: x["distance_km"], default=None)

    long_run_pace = (
        sum(r["pace_sec_per_km"] for r in sorted(long_runs, key=lambda x: x["distance_km"], reverse=True)[:3])
        / max(1, len(sorted(long_runs, key=lambda x: x["distance_km"], reverse=True)[:3]))
        if long_runs
        else None
    )
    medium_run_pace = (
        sum(r["pace_sec_per_km"] for r in medium_runs) / len(medium_runs)
        if medium_runs
        else None
    )

    latest_metric = fetch_latest_metric(user_id)
    ctl = round(float(latest_metric.ctl), 1) if latest_metric else 0.0
    atl = round(float(latest_metric.atl), 1) if latest_metric else 0.0
    tsb = round(float(latest_metric.tsb), 1) if latest_metric else 0.0

    baseline_goal = _baseline_weekly_goal(goal_context["goal_seconds"])
    weekly_goal_km = round(max(baseline_goal, avg_weekly_km * 1.10), 1)
    remaining = max(0.0, round(weekly_goal_km - current_week_km, 1))
    progress = int(min(100, (current_week_km / weekly_goal_km) * 100)) if weekly_goal_km > 0 else 0
    days_left = max(1, 6 - today.weekday())
    runs_remaining = max(1, (days_left + 1) // 2)
    avg_run_needed = round(remaining / runs_remaining, 1) if runs_remaining else remaining

    lrr = None
    lrr_status = "Insufficient data"
    lrr_warning = None
    if longest_recent and current_week_km > 0:
        lrr = round(longest_recent["distance_km"] / current_week_km, 3)
        if lrr < 0.25:
            lrr_status = "Endurance stimulus too low"
        elif lrr <= 0.35:
            lrr_status = "Ideal"
        elif lrr <= 0.40:
            lrr_status = "Aggressive"
        else:
            lrr_status = "Fatigue risk"
        if lrr > 0.45:
            lrr_warning = "Long run too large relative to weekly mileage."

    adi = None
    adi_status = "Insufficient data"
    if len(medium_runs) >= 3 and long_runs:
        long_pace = sum(r["pace_sec_per_km"] for r in long_runs) / len(long_runs)
        adi = round(long_pace / medium_run_pace, 3) if medium_run_pace else None
        if adi is not None:
            if adi <= 1.03:
                adi_status = "Elite durability"
            elif adi <= 1.06:
                adi_status = "Strong endurance"
            elif adi <= 1.10:
                adi_status = "Moderate"
            else:
                adi_status = "Weak endurance"

    # FRI requires split paces (first half vs second half). Not available in current storage.
    fri = None
    fri_status = "Insufficient data"

    readiness_medium_ok = len(medium_runs) >= 3
    readiness_long_ok = len(long_runs) >= 2
    readiness_weekly_ok = consistent_weeks >= 3

    readiness_items = []
    need_medium = max(0, 3 - len(medium_runs))
    readiness_items.append(
        {
            "label": "Medium runs (8-12 km)",
            "current": len(medium_runs),
            "target": 3,
            "ready": readiness_medium_ok,
            "message": None if readiness_medium_ok else f"Need {need_medium} more medium run(s) between 8-12 km.",
        }
    )
    need_long = max(0, 2 - len(long_runs))
    readiness_items.append(
        {
            "label": "Long runs (18-30 km)",
            "current": len(long_runs),
            "target": 2,
            "ready": readiness_long_ok,
            "message": None if readiness_long_ok else f"Need {need_long} more long run(s) between 18-30 km.",
        }
    )
    need_weeks = max(0, 3 - consistent_weeks)
    readiness_items.append(
        {
            "label": "Weekly mileage consistency",
            "current": consistent_weeks,
            "target": 3,
            "ready": readiness_weekly_ok,
            "message": None if readiness_weekly_ok else f"Need {need_weeks} more week(s) of consistent mileage.",
        }
    )

    readiness_ready = readiness_medium_ok and readiness_long_ok and readiness_weekly_ok

    guardrail_ok = (
        longest_recent is not None
        and longest_recent["distance_km"] >= 12
        and avg_weekly_km >= 20
        and long_run_pace is not None
        and len(runs_recent) >= 6
    )

    return {
        "runs_recent_count": len(runs_recent),
        "avg_weekly_km": avg_weekly_km,
        "current_week_km": current_week_km,
        "weekly_goal_km": weekly_goal_km,
        "remaining_km": remaining,
        "progress": progress,
        "runs_remaining": runs_remaining,
        "avg_run_needed": avg_run_needed,
        "longest_recent": longest_recent,
        "longest_16w": longest_16w,
        "long_run_pace_sec_per_km": long_run_pace,
        "medium_run_pace_sec_per_km": medium_run_pace,
        "ctl": ctl,
        "atl": atl,
        "tsb": tsb,
        "lrr": lrr,
        "lrr_status": lrr_status,
        "lrr_warning": lrr_warning,
        "adi": adi,
        "adi_status": adi_status,
        "fri": fri,
        "fri_status": fri_status,
        "readiness": {
            "ready": readiness_ready,
            "items": readiness_items,
        },
        "guardrail_ok": guardrail_ok,
    }


def _prediction_layer(user_id, goal_context, metrics):
    if not metrics["readiness"]["ready"] or not metrics["guardrail_ok"]:
        return {
            "valid": False,
            "current_projection": "--",
            "race_day_projection": "--",
            "probability": 0,
            "gap_to_goal": "--",
            "note": "Not enough training data to estimate race performance.",
        }

    base_time = metrics["long_run_pace_sec_per_km"] * MARATHON_KM
    predicted_flat = base_time * _fatigue_multiplier(metrics["avg_weekly_km"])
    projected_race = predicted_flat * goal_context["elevation_factor"]

    prev = get_latest_prediction(user_id)
    if prev:
        projected_race = (0.7 * float(prev.projection_seconds)) + (0.3 * projected_race)

    save_prediction(user_id, projected_race)

    current_flat = projected_race / goal_context["elevation_factor"]
    gap = projected_race - goal_context["goal_seconds"]
    probability = int(max(5, min(95, (goal_context["goal_seconds"] / max(1.0, projected_race)) * 100)))

    return {
        "valid": True,
        "current_projection": _fmt_hms(current_flat),
        "race_day_projection": _fmt_hms(projected_race),
        "probability": probability,
        "gap_to_goal": _fmt_gap(gap),
        "note": "Projection is based on long-run pace, weekly mileage, and stability smoothing.",
        "goal_progress_pct": int(max(0, min(100, (goal_context["goal_seconds"] / projected_race) * 100))),
    }


def performance_intelligence(user_id):
    goal = get_goal(user_id)
    if not goal:
        return None

    goal_ctx = build_goal_context(goal)
    metrics = _metrics_layer(user_id, goal_ctx)
    prediction = _prediction_layer(user_id, goal_ctx, metrics)

    if goal_ctx["distance_km"] <= 10:
        target_ctl = 45
    elif goal_ctx["distance_km"] <= 21.1:
        target_ctl = 55
    else:
        target_ctl = 62

    longest = metrics["longest_recent"]
    long_run_progress = {
        "longest_km": round(longest["distance_km"], 1) if longest else 0.0,
        "longest_date": longest["date"].isoformat() if longest else None,
        "next_milestone_km": 24 if longest and longest["distance_km"] < 24 else 28 if longest and longest["distance_km"] < 28 else 32,
        "progress": min(100, int((metrics["current_week_km"] / max(1.0, metrics["weekly_goal_km"])) * 100)),
    }

    return {
        "goal": goal_ctx,
        "current_projection": prediction["current_projection"],
        "race_day_projection": prediction["race_day_projection"],
        "probability": prediction["probability"],
        "gap_to_goal": prediction["gap_to_goal"],
        "prediction_note": prediction["note"],
        "goal_progress_pct": prediction.get("goal_progress_pct", 0),
        "insufficient_data": not prediction["valid"],
        "current_ctl": metrics["ctl"],
        "target_ctl": target_ctl,
        "weekly": {
            "weekly_goal_km": metrics["weekly_goal_km"],
            "completed_km": metrics["current_week_km"],
            "remaining_km": metrics["remaining_km"],
            "progress": metrics["progress"],
            "runs_remaining": metrics["runs_remaining"],
            "avg_run_needed": metrics["avg_run_needed"],
        },
        "prediction_readiness": metrics["readiness"],
        "endurance": {
            "lrr": metrics["lrr"],
            "lrr_status": metrics["lrr_status"],
            "lrr_color": _status_color(metrics["lrr_status"]),
            "lrr_warning": metrics["lrr_warning"],
            "adi": metrics["adi"],
            "adi_status": metrics["adi_status"],
            "adi_color": _status_color(metrics["adi_status"]),
            "fri": metrics["fri"],
            "fri_status": metrics["fri_status"],
            "fri_color": _status_color(metrics["fri_status"]),
        },
        "long_run": long_run_progress,
    }


def weekly_training_summary(user_id):
    metrics = fetch_metrics(user_id)
    if not metrics:
        return {"ctl_trend": 0.0, "load_risk": "Green"}

    latest = metrics[-1]
    week_ago = metrics[0]
    for m in metrics:
        if m.date <= date.today() - timedelta(days=7):
            week_ago = m

    ctl_trend = round(float(latest.ctl) - float(week_ago.ctl), 1)

    tsb = float(latest.tsb)
    if tsb < -20:
        risk = "Red"
    elif tsb < -10:
        risk = "Yellow"
    else:
        risk = "Green"

    return {"ctl_trend": ctl_trend, "load_risk": risk}


def activity_done_today(user_id):
    today = _utc_today()
    for a in fetch_recent_activities(user_id, limit=25):
        if a.date.date() == today:
            return True
    return False


def today_training_reco(probability):
    if probability < 30:
        return {"title": "Recovery Day", "details": "Light mobility or rest", "purpose": "Recover and absorb training."}
    if probability < 55:
        return {"title": "Aerobic Run", "details": "12-14 km easy", "purpose": "Build aerobic endurance."}
    if probability < 75:
        return {"title": "Steady Endurance", "details": "14-18 km steady", "purpose": "Improve fatigue resistance."}
    return {"title": "Race-Pace Session", "details": "Marathon pace intervals", "purpose": "Sharpen race execution."}


def recent_runs(user_id, limit=5):
    rows = fetch_recent_activities(user_id, limit=30)
    out = []
    for a in rows:
        if (a.activity_type or "").lower() != "run":
            continue
        pace = (a.moving_time / a.distance_km) if a.distance_km > 0 else 0
        out.append(
            {
                "date": a.date.date().isoformat(),
                "distance": round(float(a.distance_km), 1),
                "time": _fmt_hms(a.moving_time),
                "pace": f"{int(pace//60)}:{int(pace%60):02d}/km" if pace else "--",
                "hr": int(float(a.avg_hr)) if a.avg_hr else None,
            }
        )
        if len(out) >= limit:
            break
    return out
