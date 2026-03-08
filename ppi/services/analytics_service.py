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


def _elevation_factor(elevation_type):
    factors = {
        "flat": 1.00,
        "moderate": 1.03,
        "hilly": 1.06,
        "mountain": 1.10,
    }
    return max(1.0, factors.get((elevation_type or "moderate").lower(), 1.03))


def activity_stress(activity_type, moving_time_seconds, avg_hr):
    duration_min = (moving_time_seconds or 0) / 60.0
    if duration_min <= 0:
        return 0.0

    t = (activity_type or "run").lower()
    factor = STRESS_TYPE_FACTOR.get(t, 0.70)
    hr_factor = (avg_hr / THRESHOLD_HR) if avg_hr else 0.75
    return round(duration_min * hr_factor * factor, 2)


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


def _running_training_activities(user_id, lookback_days=56):
    since = _utc_today() - timedelta(days=lookback_days)
    out = []
    for a in fetch_activities(user_id):
        run_day = a.date.date()
        if run_day < since:
            continue
        if (a.activity_type or "").lower() != "run":
            continue
        if a.distance_km > 42.2:
            continue
        if a.is_race:
            continue
        if a.distance_km <= 0 or a.moving_time <= 0:
            continue
        pace = a.moving_time / a.distance_km
        out.append(
            {
                "distance": float(a.distance_km),
                "time": float(a.moving_time),
                "pace": pace,
                "date": run_day,
                "hr": float(a.avg_hr) if a.avg_hr else None,
            }
        )
    return out


def _weekly_running_stats(user_id, weeks=8):
    today = _utc_today()
    start_week = today - timedelta(days=today.weekday())

    weekly = {}
    for i in range(weeks):
        wk = start_week - timedelta(days=7 * i)
        weekly[wk] = 0.0

    since = start_week - timedelta(days=(weeks - 1) * 7)

    for a in fetch_activities(user_id):
        d = a.date.date()
        if d < since:
            continue
        if (a.activity_type or "").lower() != "run":
            continue
        if a.is_race or a.distance_km > 42.2:
            continue

        wk = d - timedelta(days=d.weekday())
        if wk in weekly:
            weekly[wk] += float(a.distance_km)

    values = list(weekly.values())
    current_week_km = weekly.get(start_week, 0.0)

    return {
        "avg": round(sum(values) / len(values), 1) if values else 0.0,
        "current_week": round(current_week_km, 1),
        "weeks": len(values),
    }


def _target_peak_mileage(goal_time_hms):
    sec = _hms_to_seconds(goal_time_hms)
    if sec >= 4 * 3600 + 30 * 60:
        return 50.0
    if sec >= 4 * 3600:
        return 62.5
    if sec >= 3 * 3600 + 30 * 60:
        return 77.5
    return 95.0


def weekly_goal_plan(user_id, goal_context):
    stats = _weekly_running_stats(user_id, weeks=8)
    recent_avg = stats["avg"]

    peak = _target_peak_mileage(goal_context["goal_time"])
    base = recent_avg if recent_avg > 0 else 25.0
    weekly_goal_km = min(peak, round(base * 1.10, 1))

    completed = stats["current_week"]
    remaining = max(0.0, round(weekly_goal_km - completed, 1))

    today = _utc_today()
    days_left = max(1, 6 - today.weekday())
    runs_remaining = max(1, (days_left + 1) // 2)
    avg_needed = round(remaining / runs_remaining, 1) if runs_remaining else remaining

    progress = int(min(100, (completed / weekly_goal_km) * 100)) if weekly_goal_km > 0 else 0

    return {
        "weekly_goal_km": weekly_goal_km,
        "completed_km": completed,
        "remaining_km": remaining,
        "progress": progress,
        "runs_remaining": runs_remaining,
        "avg_run_needed": avg_needed,
    }


def long_run_progression(user_id):
    runs8 = _running_training_activities(user_id, lookback_days=56)
    if not runs8:
        return {
            "longest_km": 0.0,
            "longest_date": None,
            "next_milestone_km": 10,
            "progress": 0,
        }

    longest = max(runs8, key=lambda r: r["distance"])
    longest_km = float(longest["distance"])

    milestones = [10, 12, 14, 16, 18, 21.1, 24, 27, 30, 32]
    next_milestone = 32
    for m in milestones:
        if longest_km < m:
            next_milestone = m
            break

    previous_markers = [m for m in milestones if m < next_milestone]
    prev_milestone = previous_markers[-1] if previous_markers else 0
    denom = max(0.1, next_milestone - prev_milestone)
    progress = int(max(0, min(100, ((longest_km - prev_milestone) / denom) * 100)))

    return {
        "longest_km": round(longest_km, 1),
        "longest_date": longest["date"].isoformat(),
        "next_milestone_km": next_milestone,
        "progress": progress,
    }


def endurance_metrics(user_id):
    runs8 = _running_training_activities(user_id, lookback_days=56)
    weekly = _weekly_running_stats(user_id, weeks=8)

    weekly_km = max(0.1, weekly["current_week"])
    longest = max([r["distance"] for r in runs8], default=0.0)
    lrr = round(longest / weekly_km, 3)

    if lrr < 0.25:
        lrr_band = "Endurance stimulus too low"
    elif lrr <= 0.35:
        lrr_band = "Ideal marathon preparation"
    elif lrr <= 0.40:
        lrr_band = "Aggressive training"
    else:
        lrr_band = "Fatigue risk"

    medium = [r for r in runs8 if 8 <= r["distance"] <= 12]
    long_runs = [r for r in runs8 if 18 <= r["distance"] <= 30]

    med_pace = sum(r["pace"] for r in medium) / len(medium) if medium else None
    long_pace = sum(r["pace"] for r in long_runs) / len(long_runs) if long_runs else None

    adi = round(long_pace / med_pace, 3) if med_pace and long_pace else None
    if adi is None:
        adi_band = "Insufficient data"
    elif adi <= 1.03:
        adi_band = "Elite durability"
    elif adi <= 1.06:
        adi_band = "Excellent"
    elif adi <= 1.10:
        adi_band = "Moderate"
    else:
        adi_band = "Endurance weakness"

    fri_values = []
    for r in [x for x in runs8 if 18 <= x["distance"] <= 32]:
        hr_penalty = max(0.0, ((r["hr"] or 150.0) - 150.0) / 1000.0)
        distance_penalty = max(0.0, (r["distance"] - 22.0) / 400.0)
        fri_values.append(1.00 + hr_penalty + distance_penalty)

    fri = round(sum(fri_values) / len(fri_values), 3) if fri_values else None
    if fri is None:
        fri_band = "Insufficient data"
    elif fri <= 1.03:
        fri_band = "Elite fatigue resistance"
    elif fri <= 1.06:
        fri_band = "Strong endurance"
    elif fri <= 1.10:
        fri_band = "Acceptable"
    else:
        fri_band = "High marathon fade risk"

    return {
        "lrr": lrr,
        "lrr_band": lrr_band,
        "adi": adi,
        "adi_band": adi_band,
        "fri": fri,
        "fri_band": fri_band,
    }


def performance_intelligence(user_id):
    goal = get_goal(user_id)
    if not goal:
        return None

    goal_ctx = build_goal_context(goal)
    weekly = weekly_goal_plan(user_id, goal_ctx)
    endurance = endurance_metrics(user_id)
    long_run = long_run_progression(user_id)

    runs = _running_training_activities(user_id, lookback_days=56)
    latest_metric = fetch_latest_metric(user_id)
    if not runs:
        return {
            "goal": goal_ctx,
            "current_projection": None,
            "race_day_projection": None,
            "probability": 0,
            "gap_to_goal": "--",
            "current_ctl": round(float(latest_metric.ctl), 1) if latest_metric else 0,
            "target_ctl": 62,
            "ctl_description": "CTL represents long-term training fitness derived from accumulated training stress.",
            "weekly": weekly,
            "endurance": endurance,
            "long_run": long_run,
        }

    runs.sort(key=lambda x: x["distance"], reverse=True)

    long_pace = sum(r["pace"] for r in runs[:3]) / min(3, len(runs))
    avg_pace = sum(r["pace"] for r in runs) / len(runs)

    tempo_runs = [r for r in runs if 8 <= r["distance"] <= 16]
    tempo_estimate = (sum(r["pace"] for r in tempo_runs) / len(tempo_runs)) if tempo_runs else avg_pace * 0.98

    weekly_stats = _weekly_running_stats(user_id, weeks=8)
    weekly_km_factor = min(1.20, max(0.70, weekly_stats["avg"] / 60.0))

    current_ctl = float(latest_metric.ctl) if latest_metric else 0.0
    distance_goal = goal_ctx["distance_km"]

    if distance_goal <= 10:
        target_ctl = 45
    elif distance_goal <= 21.1:
        target_ctl = 55
    else:
        target_ctl = 62

    ctl_fitness = min(1.15, max(0.60, current_ctl / target_ctl if target_ctl else 1.0))

    durability_score = 1.0
    if endurance["adi"] is not None:
        if endurance["adi"] <= 1.03:
            durability_score = 0.97
        elif endurance["adi"] <= 1.06:
            durability_score = 1.00
        elif endurance["adi"] <= 1.10:
            durability_score = 1.04
        else:
            durability_score = 1.08

    base_pace = (
        0.40 * long_pace
        + 0.30 * avg_pace
        + 0.30 * tempo_estimate
    )

    base_pace = base_pace * durability_score
    base_pace = base_pace / weekly_km_factor
    base_pace = base_pace / ctl_fitness

    current_flat_seconds = base_pace * distance_goal

    race_course_seconds_raw = current_flat_seconds * goal_ctx["elevation_factor"]

    prev = get_latest_prediction(user_id)
    if prev:
        stable_projection = (0.7 * float(prev.projection_seconds)) + (0.3 * race_course_seconds_raw)
    else:
        stable_projection = race_course_seconds_raw

    save_prediction(user_id, stable_projection)

    race_course_seconds = max(current_flat_seconds, stable_projection)
    gap = race_course_seconds - goal_ctx["goal_seconds"]

    ratio = goal_ctx["goal_seconds"] / max(1.0, race_course_seconds)
    probability = int(max(5, min(95, ratio * 100)))

    return {
        "goal": goal_ctx,
        "current_projection": _fmt_hms(current_flat_seconds),
        "race_day_projection": _fmt_hms(race_course_seconds),
        "probability": probability,
        "gap_to_goal": _fmt_gap(gap),
        "current_ctl": round(current_ctl, 1),
        "target_ctl": target_ctl,
        "ctl_description": "CTL represents long-term training fitness derived from accumulated training stress.",
        "weekly": weekly,
        "endurance": endurance,
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
    if probability < 40:
        return {"title": "Aerobic Run", "details": "12-16 km easy", "purpose": "Build endurance durability."}
    if probability < 60:
        return {"title": "Steady Long Run", "details": "16-22 km steady", "purpose": "Improve marathon stamina."}
    if probability < 75:
        return {"title": "Tempo Session", "details": "6-10 km controlled tempo", "purpose": "Improve sustained speed."}
    return {"title": "Race-Pace Blocks", "details": "Marathon pace intervals", "purpose": "Sharpen race execution."}


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




