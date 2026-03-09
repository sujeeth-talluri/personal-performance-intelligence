from datetime import date, datetime, timedelta, timezone
from math import ceil
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..repositories import (
    fetch_activities,
    fetch_latest_activity,
    fetch_latest_metric,
    fetch_metrics,
    fetch_recent_activities,
    get_goal,
    get_latest_prediction,
    save_prediction,
)

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

def _resolve_timezone(user_timezone=None):
    tz_name = (user_timezone or "Asia/Kolkata").strip()
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        fallback = {
            "asia/kolkata": timezone(timedelta(hours=5, minutes=30)),
            "asia/calcutta": timezone(timedelta(hours=5, minutes=30)),
            "utc": timezone.utc,
            "etc/utc": timezone.utc,
        }
        return fallback.get(tz_name.lower(), timezone.utc)


def _today_local(user_timezone=None):
    tz = _resolve_timezone(user_timezone)
    return datetime.now(timezone.utc).astimezone(tz).date()


def _to_local_date(dt_value, user_timezone=None):
    tz = _resolve_timezone(user_timezone)
    if dt_value.tzinfo is None:
        dt_utc = dt_value.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_value.astimezone(timezone.utc)
    return dt_utc.astimezone(tz).date()


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
    factors = {"flat": 1.00, "moderate": 1.02, "hilly": 1.05, "mountain": 1.10}
    return max(1.0, factors.get((elevation_type or "moderate").lower(), 1.02))


def _status_color(status):
    s = (status or "").lower()
    if "unavailable" in s or "insufficient" in s:
        return "grey"
    if any(k in s for k in ["strong", "excellent", "ideal"]):
        return "green"
    if any(k in s for k in ["moderate", "developing", "caution"]):
        return "orange"
    if any(k in s for k in ["risk", "weak"]):
        return "red"
    return "grey"


def _activity_to_raw(a, user_timezone=None):
    d = float(a.distance_km or 0.0)
    t = float(a.moving_time or 0.0)
    pace = (t / d) if d > 0 and t > 0 else None
    return {
        "id": a.strava_activity_id,
        "date": _to_local_date(a.date, user_timezone),
        "type": (a.activity_type or "").lower(),
        "is_race": bool(a.is_race),
        "distance_km": d,
        "moving_time_sec": t,
        "pace_sec_per_km": pace,
        "avg_hr": float(a.avg_hr) if a.avg_hr else None,
    }


def _raw_layer(user_id, user_timezone=None):
    seen = set()
    out = []
    for a in fetch_activities(user_id):
        r = _activity_to_raw(a, user_timezone)
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        out.append(r)
    return out


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


def _calendar_week_bounds(today):
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start, end


def _calendar_week_distance(distance_activities, week_start, week_end):
    return round(
        sum(a["distance_km"] for a in distance_activities if week_start <= a["date"] <= week_end),
        1,
    )


def _rolling_week_consistency(distance_activities, today):
    week_start, _ = _calendar_week_bounds(today)
    weekly_totals = []
    for i in range(4):
        start = week_start - timedelta(days=7 * i)
        end = start + timedelta(days=6)
        weekly_totals.append(_calendar_week_distance(distance_activities, start, end))
    return weekly_totals, len([v for v in weekly_totals if v >= 25.0])

def _ctl_proxy_6w(distance_activities, today):
    windows = [_sum_distance_in_window(distance_activities, today - timedelta(days=7 * i), days=7) for i in range(6)]
    return round(sum(windows) / len(windows), 1) if windows else 0.0

def _daily_distance_series(distance_activities, today, days=14):
    start = today - timedelta(days=days - 1)
    out = []
    for i in range(days):
        day = start + timedelta(days=i)
        km = round(sum(a["distance_km"] for a in distance_activities if a["date"] == day), 1)
        out.append({"date": day.isoformat(), "label": f"{day.strftime('%b')} {day.day}", "value": km})
    return out


def _ctl_series(metric_rows, today, days=14):
    start = today - timedelta(days=days - 1)
    by_date = {m.date: float(m.ctl) for m in metric_rows}
    last_known = 0.0
    out = []
    for i in range(days):
        day = start + timedelta(days=i)
        if day in by_date:
            last_known = by_date[day]
        out.append({"date": day.isoformat(), "label": f"{day.strftime('%b')} {day.day}", "value": round(last_known, 1)})
    return out

def _fit_half_equivalent(pace_medium, pace_long):
    if pace_medium and pace_long:
        return (0.55 * pace_long + 0.45 * pace_medium) * 21.1
    if pace_long:
        return pace_long * 21.1
    if pace_medium:
        return pace_medium * 21.1
    return None


def _fri_from_runs(long_runs):
    # Requires split-level paces not stored in current DB schema.
    # If future ingestion adds split paces, this computation auto-enables.
    if len(long_runs) < 3:
        return None

    ratios = []
    for r in long_runs:
        first = r.get("first_half_pace_sec_per_km")
        second = r.get("second_half_pace_sec_per_km")
        if not first or not second:
            continue
        if second <= 0:
            continue
        ratios.append(first / second)

    if len(ratios) < 3:
        return None
    return round(sum(ratios[-3:]) / 3.0, 3)


def build_goal_context(goal, today_local=None):
    goal_seconds = _hms_to_seconds(goal.goal_time)
    base_day = today_local or date.today()
    days_remaining = (goal.race_date - base_day).days
    pace_sec = goal_seconds / float(goal.race_distance)
    return {
        "race_name": goal.race_name,
        "distance_km": float(goal.race_distance),
        "goal_time": goal.goal_time,
        "goal_seconds": goal_seconds,
        "race_date": goal.race_date.isoformat(),
        "days_remaining": days_remaining,
        "target_pace": f"{int(pace_sec // 60)}:{int(pace_sec % 60):02d}/km",
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


def _metrics_layer(user_id, goal_ctx, user_timezone=None):
    raw = _raw_layer(user_id, user_timezone)
    today = _today_local(user_timezone)

    distance_acts = _distance_training_activities(raw)
    runs = _prediction_runs(raw)
    runs_8w = [r for r in runs if r["date"] >= today - timedelta(days=56)]

    medium_runs = [r for r in runs_8w if 8 <= r["distance_km"] <= 12]
    long_runs = [r for r in runs_8w if 18 <= r["distance_km"] <= 30]

    week_start, week_end = _calendar_week_bounds(today)
    rolling_week_distance_km = _calendar_week_distance(distance_acts, week_start, week_end)
    _, consistent_weeks = _rolling_week_consistency(distance_acts, today)
    ctl_proxy = _ctl_proxy_6w(distance_acts, today)
    distance_series_14 = _daily_distance_series(distance_acts, today, days=14)

    baseline_goal = _baseline_weekly_goal(goal_ctx["goal_seconds"])
    weekly_goal_km = round(max(baseline_goal, rolling_week_distance_km * 1.10), 1)
    completed_km = rolling_week_distance_km
    remaining_km = round(max(0.0, weekly_goal_km - completed_km), 1)

    longest_run = max(runs_8w, key=lambda x: x["distance_km"], default=None)
    long_run_km = longest_run["distance_km"] if longest_run else 0.0
    max_safe_run = min(long_run_km * 1.1 if long_run_km > 0 else weekly_goal_km * 0.35, weekly_goal_km * 0.35)
    max_safe_run = round(max(5.0, max_safe_run), 1)

    days_remaining_in_week = 6 - today.weekday()
    today_activity_logged = any(a["date"] == today for a in distance_acts)
    week_closed = today_activity_logged and days_remaining_in_week == 0
    remaining_training_days = max(0, days_remaining_in_week + (0 if today_activity_logged else 1))

    if remaining_km <= 0:
        suggested_runs = 0
        avg_run_needed = 0.0
        guidance_text = "Goal achieved."
    else:
        suggested_runs = max(1, remaining_training_days)
        avg_run_needed = round(remaining_km / suggested_runs, 1)
        guidance_text = None

    progress = int(min(100, (completed_km / weekly_goal_km) * 100)) if weekly_goal_km > 0 else 0

    lrr = None
    lrr_status = "Insufficient data"
    lrr_warning = None
    lrr_message = None
    if completed_km < 10:
        lrr_status = "Unavailable this week"
        lrr_message = "Long run ratio will update after this week's runs."
    elif longest_run and completed_km > 0:
        lrr = round(longest_run["distance_km"] / completed_km, 3)
        if lrr < 0.25:
            lrr_status = "Endurance stimulus too low"
        elif lrr <= 0.35:
            lrr_status = "Ideal"
        elif lrr <= 0.40:
            lrr_status = "Aggressive"
        else:
            lrr_status = "Fatigue risk"
        if lrr > 0.40:
            lrr_warning = "Long run too large relative to weekly mileage."

    adi = None
    adi_status = "Insufficient data"
    pace_medium = (sum(r["pace_sec_per_km"] for r in medium_runs) / len(medium_runs)) if medium_runs else None
    pace_long = (sum(r["pace_sec_per_km"] for r in long_runs) / len(long_runs)) if long_runs else None
    if pace_medium and pace_long:
        adi = round(pace_long / pace_medium, 3)
        if adi >= 1.00:
            adi_status = "Strong endurance"
        elif adi >= 0.97:
            adi_status = "Moderate"
        else:
            adi_status = "Developing"

    fri = _fri_from_runs(long_runs)
    if fri is None:
        fri_status = "FRI unavailable"
        fri_message = "Need 3 long runs >=18 km in last 8 weeks."
    elif fri > 0.97:
        fri_status = "Excellent fatigue resistance"
        fri_message = None
    elif fri >= 0.94:
        fri_status = "Good"
        fri_message = None
    elif fri >= 0.90:
        fri_status = "Moderate"
        fri_message = None
    else:
        fri_status = "Weak endurance durability"
        fri_message = None

    need_medium = max(0, 3 - len(medium_runs))
    need_long = max(0, 2 - len(long_runs))
    need_weeks = max(0, 3 - consistent_weeks)
    need_weekly = 0 if rolling_week_distance_km >= 45 else 1

    readiness_items = [
        {
            "title": "Medium runs",
            "done": len(medium_runs),
            "min": 3,
            "ready": need_medium == 0,
            "line1": "Medium runs requirement satisfied" if need_medium == 0 else "Medium runs requirement not met",
            "line2": f"{len(medium_runs)} runs completed (minimum needed: 3)",
            "line3": None if need_medium == 0 else f"Need {need_medium} more medium run(s) between 8-12 km.",
            "weight": 0.25,
            "score": min(len(medium_runs) / 3.0, 1.0),
        },
        {
            "title": "Long runs",
            "done": len(long_runs),
            "min": 2,
            "ready": need_long == 0,
            "line1": "Long runs requirement satisfied" if need_long == 0 else "Long runs requirement not met",
            "line2": f"{len(long_runs)} runs completed (minimum needed: 2)",
            "line3": None if need_long == 0 else "Next qualifying long run needed (18-30 km). Suggested: this weekend.",
            "weight": 0.25,
            "score": min(len(long_runs) / 2.0, 1.0),
        },
        {
            "title": "Consistent mileage weeks",
            "done": consistent_weeks,
            "min": 3,
            "ready": need_weeks == 0,
            "line1": "Consistent mileage requirement satisfied" if need_weeks == 0 else "Consistent mileage weeks",
            "line2": f"{consistent_weeks} completed",
            "line3": None if need_weeks == 0 else f"{need_weeks} more week required",
            "weight": 0.25,
            "score": min(consistent_weeks / 3.0, 1.0),
        },
        {
            "title": "Weekly mileage",
            "done": rolling_week_distance_km,
            "min": 45,
            "ready": need_weekly == 0,
            "line1": "Weekly mileage threshold satisfied" if need_weekly == 0 else "Weekly mileage threshold not met",
            "line2": f"{rolling_week_distance_km} km completed (minimum needed: 45 km)",
            "line3": None if need_weekly == 0 else "Need to reach 45 km weekly mileage.",
            "weight": 0.25,
            "score": min(rolling_week_distance_km / 45.0, 1.0),
        },
    ]

    readiness_progress = int(round(sum(i["weight"] * i["score"] for i in readiness_items) * 100))
    next_requirement = next((i["line3"] for i in readiness_items if not i["ready"] and i.get("line3")), None)
    if week_closed and rolling_week_distance_km < 45.0:
        next_requirement = "Prediction unlock expected after next qualifying week."

    latest_metric = fetch_latest_metric(user_id)
    ctl_real = round(float(latest_metric.ctl), 1) if latest_metric else 0.0

    return {
        "medium_runs": medium_runs,
        "long_runs": long_runs,
        "pace_medium": pace_medium,
        "pace_long": pace_long,
        "ctl_proxy": ctl_proxy,
        "ctl_real": ctl_real,
        "distance_series_14": distance_series_14,
        "readiness": {"ready": all(i["ready"] for i in readiness_items), "items": readiness_items},
        "readiness_progress_pct": readiness_progress,
        "next_requirement": next_requirement,
        "weekly": {
            "weekly_goal_km": weekly_goal_km,
            "completed_km": completed_km,
            "remaining_km": remaining_km,
            "progress": progress,
            "runs_remaining": suggested_runs,
            "avg_run_needed": avg_run_needed,
            "guidance": guidance_text,
            "max_safe_run": max_safe_run,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "days_remaining_in_week": days_remaining_in_week,
            "remaining_training_days": remaining_training_days,
            "today_activity_logged": today_activity_logged,
            "week_closed": week_closed,
        },
        "longest_run": longest_run,
        "endurance": {
            "lrr": lrr,
            "lrr_status": lrr_status,
            "lrr_color": _status_color(lrr_status),
            "lrr_warning": lrr_warning,
            "lrr_message": lrr_message,
            "adi": adi,
            "adi_status": adi_status,
            "adi_color": _status_color(adi_status),
            "fri": fri,
            "fri_status": fri_status,
            "fri_color": _status_color(fri_status),
            "fri_message": fri_message,
        },
    }


def _marathon_prediction_seconds(metrics):
    half_equiv = _fit_half_equivalent(metrics["pace_medium"], metrics["pace_long"])
    if not half_equiv:
        return None

    fri = metrics["endurance"]["fri"]
    if fri is None:
        fatigue_factor = 1.08
    elif fri > 0.97:
        fatigue_factor = 1.02
    elif fri >= 0.94:
        fatigue_factor = 1.05
    elif fri >= 0.90:
        fatigue_factor = 1.07
    else:
        fatigue_factor = 1.10

    marathon_time = half_equiv * 2.1 * fatigue_factor

    weekly = metrics["weekly"]["completed_km"]
    if weekly >= 65:
        marathon_time *= 0.98
    elif weekly < 45:
        marathon_time *= 1.03

    return marathon_time


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
        reason = metrics["next_requirement"] or "Prediction not unlocked yet."
        return {
            "valid": False,
            "current_projection": "Prediction not calculated yet",
            "race_day_projection": "Prediction not calculated yet",
            "probability": None,
            "gap_to_goal": "--",
            "goal_progress_pct": None,
            "note": reason,
        }

    new_flat = _marathon_prediction_seconds(metrics)
    if not new_flat:
        return {
            "valid": False,
            "current_projection": "Prediction not calculated yet",
            "race_day_projection": "Prediction not calculated yet",
            "probability": None,
            "gap_to_goal": "--",
            "goal_progress_pct": None,
            "note": "Not enough training data to estimate race performance.",
        }

    prev = get_latest_prediction(user_id)
    flat = (0.7 * float(prev.projection_seconds) + 0.3 * new_flat) if prev else new_flat

    should_persist = prev is None
    if prev is not None:
        latest_signal_dt = None

        latest_activity = fetch_latest_activity(user_id)
        if latest_activity and latest_activity.date:
            latest_signal_dt = latest_activity.date

        latest_metric = fetch_latest_metric(user_id)
        if latest_metric and latest_metric.date:
            metric_dt = datetime.combine(latest_metric.date, datetime.min.time())
            latest_signal_dt = max(latest_signal_dt, metric_dt) if latest_signal_dt else metric_dt

        if latest_signal_dt and latest_signal_dt > prev.created_at:
            should_persist = True

    if should_persist:
        save_prediction(user_id, flat)

    race_proj = flat * goal_ctx["elevation_factor"]
    gap = race_proj - goal_ctx["goal_seconds"]

    return {
        "valid": True,
        "current_projection": _fmt_hms(flat),
        "race_day_projection": _fmt_hms(race_proj),
        "probability": _goal_probability(race_proj, goal_ctx["goal_seconds"]),
        "gap_to_goal": _fmt_gap(gap),
        "goal_progress_pct": int(max(0, min(100, (goal_ctx["goal_seconds"] / race_proj) * 100))),
        "note": "Marathon estimate uses medium pace, long-run pace, weekly volume, and fatigue factor.",
    }


def performance_intelligence(user_id, user_timezone=None):
    goal = get_goal(user_id)
    if not goal:
        return None

    today_local = _today_local(user_timezone)
    goal_ctx = build_goal_context(goal, today_local=today_local)
    metrics = _metrics_layer(user_id, goal_ctx, user_timezone=user_timezone)
    prediction = _prediction_layer(user_id, goal_ctx, metrics)

    if goal_ctx["distance_km"] <= 10:
        target_ctl = 45
    elif goal_ctx["distance_km"] <= 21.1:
        target_ctl = 55
    else:
        target_ctl = 62

    weekly = metrics["weekly"]
    longest = metrics["longest_run"]

    training_status = {
        "title": "Training Status",
        "summary": "Race prediction unlocking after next qualifying week." if not metrics["readiness"]["ready"] else "Prediction ready",
        "detail": prediction["note"],
        "progress_pct": metrics["readiness_progress_pct"],
        "next_requirement": metrics["next_requirement"],
    }

    ctl_progress_pct = int(max(0, min(100, (metrics["ctl_real"] / max(1.0, target_ctl)) * 100)))

    all_metrics = fetch_metrics(user_id)
    ctl_series_14 = _ctl_series(all_metrics, today_local, days=14)

    if len(all_metrics) >= 2:
        latest_metric_obj = all_metrics[-1]
        latest_ctl = float(latest_metric_obj.ctl)

        baseline_cutoff = latest_metric_obj.date - timedelta(days=14)
        baseline_metric = all_metrics[0]
        for m in all_metrics:
            if m.date <= baseline_cutoff:
                baseline_metric = m
            else:
                break

        delta = round(latest_ctl - float(baseline_metric.ctl), 1)
        if delta > 0.5:
            ctl_trend_text = f"Trend: up +{delta} last 14 days"
        elif delta < -0.5:
            ctl_trend_text = f"Trend: down {delta} last 14 days"
        else:
            ctl_trend_text = "Trend: stable"
    else:
        ctl_trend_text = "Trend: stable"

    if metrics["weekly"]["completed_km"] >= 60:
        aerobic_base = "Strong"
    elif metrics["weekly"]["completed_km"] >= 45:
        aerobic_base = "Moderate"
    else:
        aerobic_base = "Developing"

    endurance_depth = "Strong" if longest and longest["distance_km"] >= 26 else "Moderate" if longest and longest["distance_km"] >= 21 else "Developing"
    fatigue_resistance = "Developing" if metrics["endurance"]["fri"] is None else metrics["endurance"]["fri_status"].replace("fatigue resistance", "").strip().title()

    if aerobic_base == "Strong" and endurance_depth == "Strong":
        overall = "Strong"
    elif aerobic_base == "Developing" or endurance_depth == "Developing":
        overall = "Building"
    else:
        overall = "Moderate"

    race_readiness = {
        "aerobic_base": aerobic_base,
        "endurance_depth": endurance_depth,
        "fatigue_resistance": fatigue_resistance,
        "overall": overall,
    }

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
        "ctl_progress_pct": ctl_progress_pct,
        "ctl_trend_text": ctl_trend_text,
        "weekly": weekly,
        "prediction_readiness": metrics["readiness"],
        "endurance": metrics["endurance"],
        "training_status": training_status,
        "race_readiness": race_readiness,
        "charts": {
            "weekly_distance_14": metrics["distance_series_14"],
            "ctl_14": ctl_series_14,
        },
        "long_run": {
            "longest_km": round(longest["distance_km"], 1) if longest else 0.0,
            "longest_date": longest["date"].isoformat() if longest else None,
            "next_milestone_km": 24 if longest and longest["distance_km"] < 24 else 28 if longest and longest["distance_km"] < 28 else 32,
            "progress": min(100, int((weekly["completed_km"] / max(1.0, weekly["weekly_goal_km"])) * 100)),
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
    risk = "Red" if tsb < -20 else "Yellow" if tsb < -10 else "Green"
    return {"ctl_trend": ctl_trend, "load_risk": risk}


def activity_done_today(user_id, user_timezone=None):
    today = _today_local(user_timezone)
    for a in fetch_recent_activities(user_id, limit=25):
        if _to_local_date(a.date, user_timezone) == today:
            return True
    return False


def today_training_reco(probability, user_timezone=None):
    today = _today_local(user_timezone)
    if today.weekday() == 0:
        return {
            "title": "Easy Aerobic Run",
            "details": "8-12 km easy",
            "purpose": "Build aerobic base and start weekly mileage.",
        }

    p = probability if probability is not None else 0
    if p < 30:
        return {"title": "Aerobic Run", "details": "8-10 km easy", "purpose": "Rebuild momentum and accumulate volume."}
    if p < 55:
        return {"title": "Aerobic Run", "details": "10-14 km easy", "purpose": "Build aerobic endurance."}
    if p < 75:
        return {"title": "Steady Endurance", "details": "14-18 km steady", "purpose": "Improve fatigue resistance."}
    return {"title": "Race-Pace Session", "details": "Marathon pace intervals", "purpose": "Sharpen race execution."}


def recent_runs(user_id, limit=5, user_timezone=None):
    rows = fetch_recent_activities(user_id, limit=30)
    out = []
    for a in rows:
        t = (a.activity_type or "").lower()
        if t not in {"run", "trailrun"}:
            continue
        pace = (a.moving_time / a.distance_km) if a.distance_km > 0 else 0
        dt = _to_local_date(a.date, user_timezone)
        out.append(
            {
                "date": dt.isoformat(),
                "date_label": f"{dt.strftime('%b')} {dt.day}",
                "distance": round(float(a.distance_km), 1),
                "time": _fmt_hms(a.moving_time),
                "pace": f"{int(pace//60)}:{int(pace%60):02d}/km" if pace else "--",
                "hr": int(float(a.avg_hr)) if a.avg_hr else None,
            }
        )
        if len(out) >= limit:
            break
    return out










