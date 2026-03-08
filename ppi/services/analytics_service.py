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
        "elevation_gain": float(a.elevation_gain or 0),
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

    all_run_training = [
        r
        for r in raw
        if r["type"] == "run"
        and not r["is_race"]
        and r["distance_km"] <= 42.2
        and r["distance_km"] > 0
        and r["moving_time_sec"] > 0
    ]

    runs_recent = [r for r in all_run_training if r["date"] >= recent_since]
    runs_durability = [r for r in all_run_training if r["date"] >= durability_since]

    weekly, this_week = _weekly_buckets(runs_recent, 8)
    avg_weekly_km = round(sum(weekly.values()) / len(weekly), 1) if weekly else 0.0
    current_week_km = round(weekly.get(this_week, 0.0), 1)

    longest_recent = max(runs_recent, key=lambda x: x["distance_km"], default=None)
    longest_dur = max(runs_durability, key=lambda x: x["distance_km"], default=None)

    long_runs_for_pace = sorted([r for r in runs_recent if r["distance_km"] >= 16], key=lambda x: x["distance_km"], reverse=True)[:3]
    long_run_pace = (
        sum(r["pace_sec_per_km"] for r in long_runs_for_pace) / len(long_runs_for_pace)
        if long_runs_for_pace
        else None
    )

    medium_runs = [r for r in runs_recent if 8 <= r["distance_km"] <= 12]
    medium_run_pace = (
        sum(r["pace_sec_per_km"] for r in medium_runs) / len(medium_runs)
        if medium_runs
        else None
    )

    latest_metric = fetch_latest_metric(user_id)
    ctl = float(latest_metric.ctl) if latest_metric else 0.0
    atl = float(latest_metric.atl) if latest_metric else 0.0
    tsb = float(latest_metric.tsb) if latest_metric else 0.0

    goal_seconds = goal_context["goal_seconds"]
    baseline_goal = _baseline_weekly_goal(goal_seconds)
    weekly_goal_km = round(max(baseline_goal, avg_weekly_km * 1.10), 1)

    remaining = max(0.0, round(weekly_goal_km - current_week_km, 1))
    days_left = max(1, 6 - today.weekday())
    runs_remaining = max(1, (days_left + 1) // 2)
    avg_run_needed = round(remaining / runs_remaining, 1) if runs_remaining else remaining

    lrr = None
    lrr_band = "INSUFFICIENT DATA"
    lrr_warning = None
    if longest_recent and current_week_km > 0:
        lrr = round(longest_recent["distance_km"] / current_week_km, 3)
        if lrr < 0.25:
            lrr_band = "Endurance stimulus too low"
        elif lrr <= 0.35:
            lrr_band = "Ideal"
        elif lrr <= 0.40:
            lrr_band = "Aggressive"
        else:
            lrr_band = "Fatigue risk"
        if lrr > 0.45:
            lrr_warning = "Long run too large relative to weekly mileage."

    adi = None
    adi_band = "INSUFFICIENT DATA"
    long_runs_18_32 = [r for r in runs_recent if 18 <= r["distance_km"] <= 32]
    if len(medium_runs) >= 3 and long_runs_18_32:
        lr_pace = sum(r["pace_sec_per_km"] for r in long_runs_18_32) / len(long_runs_18_32)
        adi = round(lr_pace / medium_run_pace, 3) if medium_run_pace else None
        if adi is not None:
            if adi <= 1.03:
                adi_band = "Elite durability"
            elif adi <= 1.06:
                adi_band = "Strong endurance"
            elif adi <= 1.10:
                adi_band = "Moderate"
            else:
                adi_band = "Weak endurance"

    fri = None
    fri_band = "INSUFFICIENT DATA"
    if len(long_runs_18_32) >= 2:
        fri_band = "INSUFFICIENT DATA"

    insufficient_prediction = False
    insuff_reason = None
    if len(runs_recent) < 6:
        insufficient_prediction = True
        insuff_reason = "Not enough training data to estimate race performance."
    elif not longest_recent or longest_recent["distance_km"] < 12:
        insufficient_prediction = True
        insuff_reason = "Not enough training data to estimate race performance."
    elif avg_weekly_km < 20:
        insufficient_prediction = True
        insuff_reason = "Not enough training data to estimate race performance."
    elif long_run_pace is None:
        insufficient_prediction = True
        insuff_reason = "Not enough training data to estimate race performance."

    progress = int(min(100, (current_week_km / weekly_goal_km) * 100)) if weekly_goal_km > 0 else 0

    return {
        "raw_count": len(raw),
        "runs_recent_count": len(runs_recent),
        "avg_weekly_km": avg_weekly_km,
        "current_week_km": current_week_km,
        "weekly_goal_km": weekly_goal_km,
        "remaining_km": remaining,
        "progress": progress,
        "runs_remaining": runs_remaining,
        "avg_run_needed": avg_run_needed,
        "longest_run_recent": longest_recent,
        "longest_run_16w": longest_dur,
        "long_run_pace_sec_per_km": long_run_pace,
        "medium_run_pace_sec_per_km": medium_run_pace,
        "ctl": round(ctl, 1),
        "atl": round(atl, 1),
        "tsb": round(tsb, 1),
        "lrr": lrr,
        "lrr_band": lrr_band,
        "lrr_warning": lrr_warning,
        "adi": adi,
        "adi_band": adi_band,
        "fri": fri,
        "fri_band": fri_band,
        "insufficient_prediction": insufficient_prediction,
        "insufficient_reason": insuff_reason,
    }


def _prediction_layer(user_id, goal_context, metrics):
    if metrics["insufficient_prediction"]:
        return {
            "valid": False,
            "current_projection": "INSUFFICIENT DATA",
            "race_day_projection": "INSUFFICIENT DATA",
            "probability": 0,
            "gap_to_goal": "--",
            "note": metrics["insufficient_reason"],
        }

    long_run_pace = metrics["long_run_pace_sec_per_km"]
    base_time = long_run_pace * MARATHON_KM
    multiplier = _fatigue_multiplier(metrics["avg_weekly_km"])
    flat_new = base_time * multiplier

    race_new = flat_new * goal_context["elevation_factor"]

    prev = get_latest_prediction(user_id)
    if prev:
        race_stable = (0.7 * float(prev.projection_seconds)) + (0.3 * race_new)
    else:
        race_stable = race_new

    save_prediction(user_id, race_stable)

    flat_stable = race_stable / max(1.0, goal_context["elevation_factor"])

    gap = race_stable - goal_context["goal_seconds"]
    probability = int(max(5, min(95, (goal_context["goal_seconds"] / max(1.0, race_stable)) * 100)))

    return {
        "valid": True,
        "current_projection": _fmt_hms(flat_stable),
        "race_day_projection": _fmt_hms(race_stable),
        "probability": probability,
        "gap_to_goal": _fmt_gap(gap),
        "note": "Projection is based on recent long-run pace, weekly mileage, and stability smoothing.",
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

    long_run = {
        "longest_km": round(metrics["longest_run_recent"]["distance_km"], 1) if metrics["longest_run_recent"] else 0.0,
        "longest_date": metrics["longest_run_recent"]["date"].isoformat() if metrics["longest_run_recent"] else None,
        "next_milestone_km": 24 if metrics["longest_run_recent"] and metrics["longest_run_recent"]["distance_km"] < 24 else 30,
        "progress": min(100, int((metrics["current_week_km"] / max(1.0, metrics["weekly_goal_km"])) * 100)),
    }

    return {
        "goal": goal_ctx,
        "current_projection": prediction["current_projection"],
        "race_day_projection": prediction["race_day_projection"],
        "probability": prediction["probability"],
        "gap_to_goal": prediction["gap_to_goal"],
        "prediction_note": prediction["note"],
        "insufficient_data": not prediction["valid"],
        "current_ctl": metrics["ctl"],
        "target_ctl": target_ctl,
        "ctl_description": "CTL represents long-term training fitness derived from accumulated training stress.",
        "weekly": {
            "weekly_goal_km": metrics["weekly_goal_km"],
            "completed_km": metrics["current_week_km"],
            "remaining_km": metrics["remaining_km"],
            "progress": metrics["progress"],
            "runs_remaining": metrics["runs_remaining"],
            "avg_run_needed": metrics["avg_run_needed"],
        },
        "endurance": {
            "lrr": metrics["lrr"],
            "lrr_band": metrics["lrr_band"],
            "lrr_warning": metrics["lrr_warning"],
            "adi": metrics["adi"],
            "adi_band": metrics["adi_band"],
            "fri": metrics["fri"],
            "fri_band": metrics["fri_band"],
        },
        "long_run": long_run,
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
        return {
            "title": "Easy Aerobic Run",
            "details": "8-12 km comfortable effort",
            "purpose": "Build consistency and aerobic base.",
        }
    if probability < 55:
        return {
            "title": "Steady Endurance Run",
            "details": "12-16 km steady aerobic",
            "purpose": "Improve endurance and fatigue resistance.",
        }
    if probability < 75:
        return {
            "title": "Tempo Session",
            "details": "2 x 15 min controlled tempo",
            "purpose": "Raise sustainable race pace.",
        }
    return {
        "title": "Race-Pace Blocks",
        "details": "Marathon-pace intervals with easy recoveries",
        "purpose": "Sharpen race execution.",
    }


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
