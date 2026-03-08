from datetime import date, datetime, timedelta, timezone
from math import ceil, exp

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
MARATHON_M = 42195.0
THRESHOLD_HR = 168
DISTANCE_ACTIVITY_TYPES = {"run", "trailrun", "walk", "hike", "ride", "swim"}

STRESS_TYPE_FACTOR = {
    "run": 1.0,
    "trailrun": 1.0,
    "walk": 0.55,
    "hike": 0.60,
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


def _status_color(status):
    s = (status or "").lower()
    if "insufficient" in s:
        return "grey"
    if any(k in s for k in ["excellent", "strong", "ideal", "elite"]):
        return "green"
    if any(k in s for k in ["moderate", "aggressive", "caution"]):
        return "orange"
    if any(k in s for k in ["risk", "weak", "too low"]):
        return "red"
    return "grey"


def _activity_to_raw(a):
    d = float(a.distance_km or 0.0)
    t = float(a.moving_time or 0.0)
    pace = (t / d) if d > 0 and t > 0 else None
    return {
        "id": a.strava_activity_id,
        "date": a.date.date(),
        "type": (a.activity_type or "").lower(),
        "is_race": bool(a.is_race),
        "distance_km": d,
        "moving_time_sec": t,
        "pace_sec_per_km": pace,
        "avg_hr": float(a.avg_hr) if a.avg_hr else None,
    }


def _raw_layer(user_id):
    raw = [_activity_to_raw(a) for a in fetch_activities(user_id)]
    seen = set()
    deduped = []
    for r in raw:
        key = r["id"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped


def _distance_training_activities(raw):
    return [
        r
        for r in raw
        if r["type"] in DISTANCE_ACTIVITY_TYPES and not r["is_race"] and 0 < r["distance_km"] <= 50.0
    ]


def _prediction_runs(raw):
    return [
        r
        for r in raw
        if r["type"] in {"run", "trailrun"}
        and not r["is_race"]
        and 0 < r["distance_km"] <= 42.2
        and r["moving_time_sec"] > 0
    ]


def _sum_distance_in_window(activities, end_day, days=7):
    start_day = end_day - timedelta(days=days - 1)
    return round(sum(a["distance_km"] for a in activities if start_day <= a["date"] <= end_day), 1)


def _rolling_week_consistency(distance_activities, today):
    anchors = [today - timedelta(days=7 * i) for i in range(4)]
    rolling = [_sum_distance_in_window(distance_activities, a, days=7) for a in anchors]
    consistent_weeks = len([v for v in rolling if v >= 25.0])
    return rolling, consistent_weeks


def _ctl_proxy_6w(distance_activities, today):
    windows = []
    for i in range(6):
        end_day = today - timedelta(days=7 * i)
        windows.append(_sum_distance_in_window(distance_activities, end_day, days=7))
    return round(sum(windows) / len(windows), 1) if windows else 0.0


def _riegel_projection(time_sec, dist_km, target_km=MARATHON_KM):
    if not time_sec or not dist_km or dist_km <= 0:
        return None
    return float(time_sec) * ((target_km / float(dist_km)) ** 1.06)


def _vdot_from_run(dist_km, time_sec):
    if dist_km <= 0 or time_sec <= 0:
        return None
    t_min = time_sec / 60.0
    v = (dist_km * 1000.0) / t_min
    vo2 = -4.60 + 0.182258 * v + 0.000104 * v * v
    pct = 0.8 + 0.1894393 * exp(-0.012778 * t_min) + 0.2989558 * exp(-0.1932605 * t_min)
    if pct <= 0:
        return None
    return vo2 / pct


def _vdot_to_marathon_time(vdot):
    if not vdot or vdot <= 0:
        return None

    def vdot_at_time(t_min):
        v = MARATHON_M / t_min
        vo2 = -4.60 + 0.182258 * v + 0.000104 * v * v
        pct = 0.8 + 0.1894393 * exp(-0.012778 * t_min) + 0.2989558 * exp(-0.1932605 * t_min)
        if pct <= 0:
            return 0
        return vo2 / pct

    lo, hi = 120.0, 420.0
    for _ in range(40):
        mid = (lo + hi) / 2.0
        if vdot_at_time(mid) > vdot:
            lo = mid
        else:
            hi = mid
    return ((lo + hi) / 2.0) * 60.0


def build_goal_context(goal):
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


def _baseline_weekly_goal(goal_seconds):
    if goal_seconds <= 3 * 3600:
        return 90.0
    if goal_seconds <= 3 * 3600 + 30 * 60:
        return 75.0
    if goal_seconds <= 4 * 3600:
        return 60.0
    return 45.0


def _metrics_layer(user_id, goal_ctx):
    raw = _raw_layer(user_id)
    today = _utc_today()

    distance_acts = _distance_training_activities(raw)
    runs = _prediction_runs(raw)

    runs_8w = [r for r in runs if r["date"] >= today - timedelta(days=56)]
    runs_6w = [r for r in runs if r["date"] >= today - timedelta(days=42)]

    medium_runs = [r for r in runs_8w if 8 <= r["distance_km"] <= 12]
    long_runs = [r for r in runs_8w if 18 <= r["distance_km"] <= 30]

    rolling_week_distance_km = _sum_distance_in_window(distance_acts, today, days=7)
    _, consistent_weeks = _rolling_week_consistency(distance_acts, today)

    avg_weekly_distance_6w = _ctl_proxy_6w(distance_acts, today)

    baseline_goal = _baseline_weekly_goal(goal_ctx["goal_seconds"])
    weekly_goal_km = round(max(baseline_goal, rolling_week_distance_km * 1.10), 1)
    completed_km = rolling_week_distance_km
    remaining_km = round(max(0.0, weekly_goal_km - completed_km), 1)

    longest_run = max(runs_8w, key=lambda x: x["distance_km"], default=None)
    long_run_km = longest_run["distance_km"] if longest_run else 0.0

    max_safe_run = min(long_run_km * 1.1 if long_run_km > 0 else weekly_goal_km * 0.35, weekly_goal_km * 0.35)
    max_safe_run = round(max(5.0, max_safe_run), 1)

    if remaining_km <= 0:
        suggested_runs = 0
        avg_run_needed = 0.0
        guidance_text = "Goal achieved."
    else:
        suggested_runs = int(max(1, ceil(remaining_km / max_safe_run)))
        avg_run_needed = round(remaining_km / suggested_runs, 1)
        guidance_text = None

    progress = int(min(100, (completed_km / weekly_goal_km) * 100)) if weekly_goal_km > 0 else 0

    lrr = None
    lrr_status = "Insufficient data"
    lrr_warning = None
    if longest_run and completed_km > 0:
        lrr = round(longest_run["distance_km"] / completed_km, 3)
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
    if len(medium_runs) >= 3 and len(long_runs) >= 1:
        medium_pace = sum(r["pace_sec_per_km"] for r in medium_runs) / len(medium_runs)
        long_pace = sum(r["pace_sec_per_km"] for r in long_runs) / len(long_runs)
        adi = round(long_pace / medium_pace, 3)
        if adi <= 1.03:
            adi_status = "Elite durability"
        elif adi <= 1.06:
            adi_status = "Strong endurance"
        elif adi <= 1.10:
            adi_status = "Moderate"
        else:
            adi_status = "Weak endurance"

    fri = None
    fri_status = "Insufficient data"
    # True FRI needs split paces not currently stored in DB.

    readiness_items = [
        {
            "label": "Medium runs (8-12 km)",
            "current": len(medium_runs),
            "target": 3,
            "ready": len(medium_runs) >= 3,
            "message": None if len(medium_runs) >= 3 else f"Need {3-len(medium_runs)} more medium run(s) between 8-12 km.",
        },
        {
            "label": "Long runs (18-30 km)",
            "current": len(long_runs),
            "target": 2,
            "ready": len(long_runs) >= 2,
            "message": None if len(long_runs) >= 2 else f"Need {2-len(long_runs)} more long run(s) between 18-30 km.",
        },
        {
            "label": "Weekly mileage consistency",
            "current": consistent_weeks,
            "target": 3,
            "ready": consistent_weeks >= 3,
            "message": None if consistent_weeks >= 3 else f"Need {3-consistent_weeks} more week(s) of consistent mileage.",
        },
    ]

    readiness_ready = all(item["ready"] for item in readiness_items)

    latest_metric = fetch_latest_metric(user_id)
    ctl_real = round(float(latest_metric.ctl), 1) if latest_metric else 0.0

    return {
        "rolling_week_distance_km": rolling_week_distance_km,
        "consistent_weeks": consistent_weeks,
        "runs_8w": runs_8w,
        "runs_6w": runs_6w,
        "medium_runs": medium_runs,
        "long_runs": long_runs,
        "weekly_goal_km": weekly_goal_km,
        "completed_km": completed_km,
        "remaining_km": remaining_km,
        "progress": progress,
        "suggested_runs_remaining": suggested_runs,
        "avg_run_needed": avg_run_needed,
        "max_safe_run": max_safe_run,
        "goal_guidance": guidance_text,
        "longest_run": longest_run,
        "ctl_proxy": avg_weekly_distance_6w,
        "ctl_real": ctl_real,
        "lrr": lrr,
        "lrr_status": lrr_status,
        "lrr_warning": lrr_warning,
        "adi": adi,
        "adi_status": adi_status,
        "fri": fri,
        "fri_status": fri_status,
        "readiness": {"ready": readiness_ready, "items": readiness_items},
    }


def _hybrid_prediction_seconds(metrics):
    runs_8w = metrics["runs_8w"]
    runs_6w = metrics["runs_6w"]

    long_runs = metrics["long_runs"]
    medium_runs = metrics["medium_runs"]

    if not long_runs:
        return None

    main_long = max(long_runs, key=lambda r: r["distance_km"])
    prediction_long_run = _riegel_projection(main_long["moving_time_sec"], main_long["distance_km"])

    medium_riegel = [
        _riegel_projection(r["moving_time_sec"], r["distance_km"])
        for r in medium_runs
        if r["moving_time_sec"] > 0
    ]
    prediction_medium_runs = sum(medium_riegel) / len(medium_riegel) if medium_riegel else prediction_long_run

    vdot_candidates = []
    for r in runs_6w:
        if 10 <= r["distance_km"] <= 30:
            vdot = _vdot_from_run(r["distance_km"], r["moving_time_sec"])
            if vdot:
                vdot_candidates.append(vdot)

    vdot_best = max(vdot_candidates) if vdot_candidates else None
    prediction_vdot = _vdot_to_marathon_time(vdot_best) if vdot_best else prediction_long_run

    capability = (
        0.45 * prediction_long_run
        + 0.35 * prediction_medium_runs
        + 0.20 * prediction_vdot
    )

    ctl_proxy = metrics["ctl_proxy"]
    if ctl_proxy < 35:
        capability *= 1.03
    elif ctl_proxy >= 45:
        capability *= 0.98

    return capability


def _goal_probability(predicted_seconds, goal_seconds):
    gap_minutes = (predicted_seconds - goal_seconds) / 60.0
    if gap_minutes <= -10:
        return 85
    if gap_minutes <= 10:
        return 60
    if gap_minutes <= 25:
        return 35
    return 10


def _prediction_layer(user_id, goal_ctx, metrics):
    if not metrics["readiness"]["ready"]:
        return {
            "valid": False,
            "current_projection": "--",
            "race_day_projection": "--",
            "probability": 0,
            "gap_to_goal": "--",
            "goal_progress_pct": 0,
            "note": "Prediction locked until readiness criteria are met.",
        }

    new_flat = _hybrid_prediction_seconds(metrics)
    if not new_flat:
        return {
            "valid": False,
            "current_projection": "--",
            "race_day_projection": "--",
            "probability": 0,
            "gap_to_goal": "--",
            "goal_progress_pct": 0,
            "note": "Not enough training data to estimate race performance.",
        }

    prev = get_latest_prediction(user_id)
    flat = (0.7 * float(prev.projection_seconds) + 0.3 * new_flat) if prev else new_flat
    save_prediction(user_id, flat)

    race_proj = flat * goal_ctx["elevation_factor"]
    gap = race_proj - goal_ctx["goal_seconds"]
    probability = _goal_probability(race_proj, goal_ctx["goal_seconds"])

    return {
        "valid": True,
        "current_projection": _fmt_hms(flat),
        "race_day_projection": _fmt_hms(race_proj),
        "probability": probability,
        "gap_to_goal": _fmt_gap(gap),
        "goal_progress_pct": int(max(0, min(100, (goal_ctx["goal_seconds"] / race_proj) * 100))),
        "note": "Hybrid model: Riegel + VDOT + training-load modifier with stability smoothing.",
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

    longest = metrics["longest_run"]

    return {
        "goal": goal_ctx,
        "current_projection": prediction["current_projection"],
        "race_day_projection": prediction["race_day_projection"],
        "probability": prediction["probability"],
        "gap_to_goal": prediction["gap_to_goal"],
        "prediction_note": prediction["note"],
        "goal_progress_pct": prediction["goal_progress_pct"],
        "insufficient_data": not prediction["valid"],
        "current_ctl": metrics["ctl_real"],
        "target_ctl": target_ctl,
        "weekly": {
            "weekly_goal_km": metrics["weekly_goal_km"],
            "completed_km": metrics["completed_km"],
            "remaining_km": metrics["remaining_km"],
            "progress": metrics["progress"],
            "runs_remaining": metrics["suggested_runs_remaining"],
            "avg_run_needed": metrics["avg_run_needed"],
            "guidance": metrics["goal_guidance"],
            "max_safe_run": metrics["max_safe_run"],
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
        "long_run": {
            "longest_km": round(longest["distance_km"], 1) if longest else 0.0,
            "longest_date": longest["date"].isoformat() if longest else None,
            "next_milestone_km": 24 if longest and longest["distance_km"] < 24 else 28 if longest and longest["distance_km"] < 28 else 32,
            "progress": min(100, int((metrics["completed_km"] / max(1.0, metrics["weekly_goal_km"])) * 100)),
        },
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
        if (a.activity_type or "").lower() not in {"run", "trailrun"}:
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
