from datetime import date, datetime, timedelta, timezone
from math import ceil
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..repositories import (
    fetch_activities,
    fetch_latest_activity,
    fetch_latest_metric,
    fetch_metrics,
    fetch_recent_activities,
    fetch_workout_logs,
    get_goal,
    get_latest_prediction,
    save_prediction,
)
from .load_engine import (
    DISTANCE_ACTIVITY_TYPES,
    RUN_ACTIVITY_TYPES,
    RUN_INTENSITY_FACTOR,
    STRESS_TYPE_FACTOR,
    classify_run_intensity as service_classify_run_intensity,
    load_model as service_load_model,
    running_stress_score as service_running_stress_score,
)
from .prediction_engine import (
    fit_half_equivalent as service_fit_half_equivalent,
    marathon_prediction_seconds as service_marathon_prediction_seconds,
    marathon_wall_analysis as service_marathon_wall_analysis,
    predict_all_distances as service_predict_all_distances,
    riegel_projection as service_riegel_projection,
    vdot_from_race as service_vdot_from_race,
    vo2max_estimate_from_runs as service_vo2max_estimate_from_runs,
    vo2max_marathon_projection as service_vo2max_marathon_projection,
)

THRESHOLD_HR = 168


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


def _fmt_range(low_seconds, high_seconds):
    return f"{_fmt_hms(low_seconds)} - {_fmt_hms(high_seconds)}"


def _fmt_minutes_range(low_seconds, high_seconds):
    low_minutes = max(0, int(round(low_seconds / 60.0)))
    high_minutes = max(low_minutes, int(round(high_seconds / 60.0)))
    if low_minutes == high_minutes:
        return f"{low_minutes} minutes"
    return f"{low_minutes}-{high_minutes} minutes"


def _goal_milestone_label(goal_seconds, distance_km):
    hours = goal_seconds / 3600.0
    if distance_km >= 40:
        if hours <= 3:
            return "Sub-3 marathon target"
        if hours <= 3.5:
            return "Sub-3:30 marathon target"
        if hours <= 4:
            return "Sub-4 marathon target"
        return None
    return None


def _confidence_label(score):
    if score >= 0.75:
        return "High"
    if score >= 0.5:
        return "Moderate"
    if score >= 0.3:
        return "Building"
    return "Low"


def _goal_alignment_label(probability):
    if probability is None:
        return "Too early to compare"
    if probability >= 75:
        return "On Track"
    if probability >= 45:
        return "Within Reach"
    if probability >= 20:
        return "Building"
    return "Stretch"


def _training_phase(days_remaining):
    weeks_to_race = max(0.0, days_remaining / 7.0)
    if weeks_to_race < 6:
        return "taper"
    if weeks_to_race <= 12:
        return "peak"
    if weeks_to_race <= 18:
        return "build"
    return "base"


def _cycle_week(days_remaining, phase):
    if phase in {"taper", "rebuild"}:
        return None
    weeks_remaining = max(0, int(days_remaining // 7))
    return (weeks_remaining % 4) + 1


def _effective_phase(days_remaining, rebuild_mode):
    base_phase = _training_phase(days_remaining)
    if rebuild_mode:
        return "rebuild", None
    cycle_week = _cycle_week(days_remaining, base_phase)
    if base_phase in {"base", "build", "peak"} and cycle_week == 4:
        return "recovery", cycle_week
    return base_phase, cycle_week


def _week_type_label(effective_phase, base_phase, days_remaining):
    if effective_phase == "rebuild":
        return "Rebuild"
    if base_phase == "taper":
        return "Race Week" if days_remaining <= 7 else "Sharpening Week"
    if effective_phase == "recovery":
        return "Cutback Week"
    if base_phase == "base":
        return "Endurance Build"
    if base_phase == "build":
        return "Specificity Build"
    if base_phase == "peak":
        return "Peak Specificity"
    return "Build Week"


def _freshness_label(tsb_today):
    if tsb_today < -20:
        return "Heavy fatigue", "You are carrying a lot of fatigue right now. Prioritize recovery before adding more quality."
    if tsb_today < -10:
        return "Loaded", "You are carrying useful training load. Keep the next quality session controlled."
    if tsb_today <= 5:
        return "Balanced", "You are carrying normal training fatigue for the current block."
    return "Fresh", "You have enough freshness to absorb a quality session well."


def _phase_minimum_goal(desired_peak, phase):
    return round(
        desired_peak
        * {
            "base": 0.45,
            "build": 0.55,
            "peak": 0.65,
            "recovery": 0.40,
            "taper": 0.30,
            "rebuild": 0.28,
        }.get(phase, 0.45),
        1,
    )


def _readiness_next_action(need_long, need_medium, need_weeks, need_weekly, weekly_readiness_target, rolling_week_distance_km, phase):
    actions = []
    if need_long > 0:
        actions.append(f"Complete {need_long} more long run{'s' if need_long > 1 else ''} of 18 km or longer")
    if need_weekly > 0 and phase != "taper":
        deficit = max(0.0, weekly_readiness_target - rolling_week_distance_km)
        actions.append(f"Build this week to about {int(round(weekly_readiness_target))} km total ({round(deficit, 1)} km still to add)")
    if need_medium > 0:
        actions.append(f"Add {need_medium} more medium run{'s' if need_medium > 1 else ''} between 8 and 12 km")
    if need_weeks > 0:
        actions.append(f"String together {need_weeks} more consistent training week{'s' if need_weeks > 1 else ''}")
    if not actions:
        return "Keep this week steady and protect the scheduled long run."
    if len(actions) == 1:
        return actions[0] + "."
    return f"{actions[0]}, then {actions[1].lower()}."


def _projection_basis_summary(metrics):
    parts = []
    if metrics.get("recent_race_runs"):
        parts.append("recent race efforts")
    if metrics.get("race_simulation_runs"):
        parts.append("marathon-specific simulation work")
    elif metrics.get("marathon_specific_runs"):
        parts.append("marathon-pace sessions")
    if metrics.get("long_runs"):
        parts.append("long runs")
    if metrics.get("medium_runs"):
        parts.append("steady medium runs")
    parts.append("recent training volume")
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return f"{', '.join(parts[:-1])}, and {parts[-1]}"


def _is_anomalous_run(raw):
    if raw["distance_km"] <= 0 or raw["moving_time_sec"] <= 0:
        return True
    pace = raw.get("pace_sec_per_km")
    if raw["type"] in {"run", "trailrun"} and pace:
        if pace < 150 or pace > 900:
            return True
    elevation_gain = raw.get("elevation_gain") or 0.0
    if raw["distance_km"] < 10 and elevation_gain > 1500:
        return True
    if raw["distance_km"] > 80:
        return True
    return False


def _weekly_distance_history(distance_activities, today, weeks=5):
    week_start, _ = _calendar_week_bounds(today)
    totals = []
    for i in range(weeks):
        start = week_start - timedelta(days=7 * i)
        end = start + timedelta(days=6)
        totals.append(_calendar_week_distance(distance_activities, start, end))
    return totals


def _gap_days(runs, today):
    if not runs:
        return 999
    latest = max(r["date"] for r in runs)
    return (today - latest).days

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



def _running_stress_score(activity, marathon_pace_sec_per_km):
    return service_running_stress_score(activity, marathon_pace_sec_per_km)
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
        "elevation_gain": float(a.elevation_gain) if a.elevation_gain else 0.0,
        "first_half_pace_sec_per_km": getattr(a, "first_half_pace_sec_per_km", None),
        "second_half_pace_sec_per_km": getattr(a, "second_half_pace_sec_per_km", None),
    }


def _classify_run_intensity(run, marathon_pace_sec_per_km):
    return service_classify_run_intensity(run, marathon_pace_sec_per_km)


def _raw_layer(user_id, user_timezone=None):
    seen = set()
    out = []
    for a in fetch_activities(user_id):
        r = _activity_to_raw(a, user_timezone)
        if r["id"] in seen:
            continue
        r["anomaly"] = _is_anomalous_run(r)
        seen.add(r["id"])
        out.append(r)
    return out


def _distance_training_activities(raw):
    return [
        r
        for r in raw
        if r["type"] in DISTANCE_ACTIVITY_TYPES
        and not r["is_race"]
        and not r.get("anomaly")
        and (r["type"] not in RUN_ACTIVITY_TYPES or r["distance_km"] >= 3.0)
        and 0 < r["distance_km"] <= 50.0
    ]


def _run_training_activities(raw):
    return [
        r
        for r in raw
        if r["type"] in RUN_ACTIVITY_TYPES and not r["is_race"] and not r.get("anomaly") and 3.0 <= r["distance_km"] <= 50.0
    ]


def _load_activities(raw):
    return [
        r
        for r in raw
        if r["type"] in STRESS_TYPE_FACTOR
        and not r["is_race"]
        and not r.get("anomaly")
        and (r["type"] not in RUN_ACTIVITY_TYPES or r["distance_km"] >= 3.0)
        and r["moving_time_sec"] > 0
        and (r["distance_km"] <= 50.0 if r["distance_km"] else True)
    ]


def _prediction_runs(raw):
    return [
        r
        for r in raw
        if r["type"] in {"run", "trailrun"}
        and not r["is_race"]
        and not r.get("anomaly")
        and 3.0 <= r["distance_km"] <= 42.2
        and r["moving_time_sec"] > 0
    ]


def _recent_gap_metrics(runs, today, window_days=42):
    if not runs:
        return {"days_since_latest": 999, "max_gap_days": 999}

    sorted_dates = sorted({r["date"] for r in runs})
    days_since_latest = (today - sorted_dates[-1]).days
    window_start = today - timedelta(days=window_days)
    window_dates = [d for d in sorted_dates if d >= window_start]
    if not window_dates:
        return {"days_since_latest": days_since_latest, "max_gap_days": days_since_latest}

    max_gap = max(
        ((window_dates[idx] - window_dates[idx - 1]).days for idx in range(1, len(window_dates))),
        default=0,
    )
    max_gap = max(max_gap, (window_dates[0] - window_start).days)
    max_gap = max(max_gap, (today - window_dates[-1]).days)
    return {"days_since_latest": days_since_latest, "max_gap_days": max_gap}


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

def _ctl_ema_series(load_activities, today, marathon_pace_sec_per_km, days=14):
    load = service_load_model(load_activities, today, marathon_pace_sec_per_km, days=days)
    return load["ctl_today"], load["ctl_series"]


def _load_model(load_activities, today, marathon_pace_sec_per_km, days=14):
    return service_load_model(load_activities, today, marathon_pace_sec_per_km, days=days)


def _daily_distance_series(distance_activities, today, days=14):
    start = today - timedelta(days=days - 1)
    out = []
    for i in range(days):
        day = start + timedelta(days=i)
        km = round(sum(a["distance_km"] for a in distance_activities if a["date"] == day), 1)
        out.append({"date": day.isoformat(), "label": f"{day.strftime('%b')} {day.day}", "value": km})
    return out


def _long_run_progress_state(runs, today):
    # Milestone ladder: a step is "done" when the runner has covered ≥ 95% of it.
    # next_step is derived purely from the longest completed run so that a run of
    # e.g. 21.8 km correctly advances to 24 km rather than staying at 21 km.
    LADDER = [21, 24, 28, 32, 35, 38, 42]
    recent = [r for r in runs if r["date"] >= today - timedelta(days=70)]
    last_long = max(recent, key=lambda r: (r["date"], r["distance_km"]), default=None)
    longest_km = last_long["distance_km"] if last_long else 0.0
    # Count how many ladder milestones are fully covered (>= 95% threshold)
    completed_count = sum(1 for step in LADDER if longest_km >= step * 0.95)
    completed_step  = max((step for step in LADDER if longest_km >= step * 0.95), default=0.0)
    next_step = next((step for step in LADDER if step > longest_km), 42)
    qualifying_longs = sorted([r for r in recent if r["distance_km"] >= 18], key=lambda r: r["date"])
    failed_recent = False
    if len(qualifying_longs) >= 2:
        previous = qualifying_longs[-2]["distance_km"]
        latest = qualifying_longs[-1]["distance_km"]
        if previous >= 21 and latest < previous * 0.9:
            failed_recent = True
    return {
        "ladder": LADDER,
        "completed_step": completed_step,
        "next_step": next_step,
        "milestones_completed": completed_count,
        "latest_long": last_long,
        "failed_recent": failed_recent,
    }


def _race_simulation_runs(runs, marathon_pace_sec_per_km):
    sims = []
    for run in runs:
        if run["distance_km"] < 20:
            continue
        pace = run.get("pace_sec_per_km")
        if not pace:
            continue
        pace_gap = abs(pace - marathon_pace_sec_per_km) / max(1.0, marathon_pace_sec_per_km)
        if pace_gap <= 0.08:
            sims.append(run)
    return sims


def _fit_half_equivalent(pace_medium, pace_long):
    return service_fit_half_equivalent(pace_medium, pace_long)


def _riegel_projection(time_seconds, distance_km, target_km=42.195, exponent=1.06):
    return service_riegel_projection(time_seconds, distance_km, target_km=target_km, exponent=exponent)


def _vo2max_estimate_from_runs(runs):
    return service_vo2max_estimate_from_runs(runs)


def _vo2max_marathon_projection(vo2max):
    return service_vo2max_marathon_projection(vo2max)


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


def _aerobic_durability_from_runs(long_runs):
    qualifying = [r for r in long_runs if r["distance_km"] >= 15]
    if not qualifying:
        return None, None

    drift_scores = []
    for run in qualifying:
        first = run.get("first_half_pace_sec_per_km")
        second = run.get("second_half_pace_sec_per_km")
        if first and second and first > 0:
            drift_scores.append((second - first) / first)

    if not drift_scores:
        paces = [r.get("pace_sec_per_km") for r in qualifying if r.get("pace_sec_per_km")]
        if not paces:
            return None, None
        avg_pace = sum(paces) / len(paces)
        stability = sum(abs(p - avg_pace) / avg_pace for p in paces) / len(paces)
        hr_values = [r.get("avg_hr") for r in qualifying if r.get("avg_hr")]
        hr_drift = 0.0
        if len(hr_values) >= 2:
            hr_avg = sum(hr_values) / len(hr_values)
            hr_drift = sum(abs(h - hr_avg) / max(1.0, hr_avg) for h in hr_values) / len(hr_values)
        pace_drop = (0.7 * stability) + (0.3 * hr_drift)
    else:
        pace_drop = sum(drift_scores) / len(drift_scores)

    if pace_drop < 0.05:
        return "Strong", pace_drop
    if pace_drop < 0.10:
        return "Moderate", pace_drop
    return "Weak", pace_drop


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
        "personal_best": goal.personal_best,  # FM PB H:MM:SS string or None
        "pb_hm":  getattr(goal, "pb_hm",  None),
        "pb_10k": getattr(goal, "pb_10k", None),
        "pb_5k":  getattr(goal, "pb_5k",  None),
    }


def _baseline_weekly_goal(goal_seconds):
    if goal_seconds <= 3 * 3600:
        return 100.0
    if goal_seconds <= 3 * 3600 + 30 * 60:
        return 85.0
    if goal_seconds <= 4 * 3600:
        return 80.0
    if goal_seconds <= 4 * 3600 + 30 * 60:
        return 65.0
    return 45.0


def _phase_goal_floor(desired_peak, phase):
    floor_factor = {
        "base": 0.50,
        "build": 0.65,
        "peak": 0.80,
        "recovery": 0.45,
        "taper": 0.30,
        "rebuild": 0.28,
    }.get(phase, 0.50)
    return round(desired_peak * floor_factor, 1)


def _safe_pb_seconds(pb_str: str | None, distance_km: float) -> float | None:
    """Parse a personal-best time string (H:MM:SS) to seconds for marathon goals."""
    if not pb_str or distance_km < 40.0:
        return None
    try:
        return _hms_to_seconds(pb_str)
    except (ValueError, AttributeError):
        return None


def _parse_time_str(pb_str: str | None) -> float | None:
    """Parse H:MM:SS or MM:SS time string to seconds (no distance restriction)."""
    if not pb_str:
        return None
    try:
        return _hms_to_seconds(pb_str)
    except (ValueError, AttributeError):
        return None


def _best_pb_vdot(goal_ctx: dict) -> float | None:
    """Return the highest VDOT derivable from any stored personal best."""
    # Race distances in metres (mirrors prediction_engine constants)
    _D = {"pb_hm": 21_097.5, "pb_10k": 10_000.0, "pb_5k": 5_000.0, "personal_best": 42_195.0}
    best = None
    for field, dist_m in _D.items():
        pb_str = goal_ctx.get(field)
        sec = _parse_time_str(pb_str)
        if not sec or sec <= 0:
            continue
        v = service_vdot_from_race(dist_m, sec / 60.0)
        if v and v >= 20:
            best = max(best, v) if best is not None else v
    return round(best, 2) if best is not None else None


def _metrics_layer(user_id, goal_ctx, user_timezone=None):
    raw = _raw_layer(user_id, user_timezone)
    today = _today_local(user_timezone)
    base_phase = _training_phase(goal_ctx["days_remaining"])

    distance_acts = _distance_training_activities(raw)
    run_distance_acts = _run_training_activities(raw)
    load_acts = _load_activities(raw)
    runs = _prediction_runs(raw)
    for run in runs:
        run["intensity"] = _classify_run_intensity(run, goal_ctx["goal_seconds"] / 42.195)
    runs_8w = [r for r in runs if r["date"] >= today - timedelta(days=56)]

    medium_runs = [
        r for r in runs_8w
        if 8 <= r["distance_km"] <= 12 and r.get("intensity") in {"steady", "tempo", "marathon_specific", "aerobic", "speed"}
    ]
    long_runs = [r for r in runs_8w if 18 <= r["distance_km"] <= 30]
    marathon_specific_runs = [
        r for r in runs_8w
        if r.get("intensity") in {"marathon_specific", "marathon_specific_long", "steady_long", "tempo"} and r["distance_km"] >= 10
    ]
    recent_race_runs = [
        r for r in raw
        if r["type"] in RUN_ACTIVITY_TYPES and r.get("is_race") and 5 <= r["distance_km"] <= 21.5 and r["date"] >= today - timedelta(days=84)
    ]
    vo2max_estimate = _vo2max_estimate_from_runs(runs_8w + recent_race_runs)

    week_start, week_end = _calendar_week_bounds(today)
    rolling_week_distance_km = _calendar_week_distance(run_distance_acts, week_start, week_end)
    cross_training_distance_km = round(
        sum(a["distance_km"] for a in distance_acts if a["type"] not in RUN_ACTIVITY_TYPES and week_start <= a["date"] <= week_end),
        1,
    )
    rolling_weeks, consistent_weeks = _rolling_week_consistency(run_distance_acts, today)
    recent_planned_logs = fetch_workout_logs(user_id, today - timedelta(days=27), today)
    planned_run_logs = [log for log in recent_planned_logs if log.workout_type == "RUN"]
    completed_run_logs = [log for log in planned_run_logs if log.status in {"completed", "moved"}]
    training_consistency_ratio = (len(completed_run_logs) / len(planned_run_logs)) if planned_run_logs else 0.0
    weekly_history = _weekly_distance_history(run_distance_acts, today, weeks=5)
    prior_weeks = weekly_history[1:] if len(weekly_history) > 1 else []
    prior_avg = round(sum(prior_weeks) / len(prior_weeks), 1) if prior_weeks else 0.0
    gap_metrics = _recent_gap_metrics(runs, today)
    gap_days = gap_metrics["days_since_latest"]
    rebuild_mode = gap_days >= 21 or (gap_metrics["max_gap_days"] >= 21 and consistent_weeks < 3)
    phase, cycle_week = _effective_phase(goal_ctx["days_remaining"], rebuild_mode)
    week_type = _week_type_label(phase, base_phase, goal_ctx["days_remaining"])
    load_model = _load_model(load_acts, today, goal_ctx["goal_seconds"] / 42.195, days=14)
    ctl_today = load_model["ctl_today"]
    ctl_series_14 = load_model["ctl_series"]
    atl_series_14 = load_model["atl_series"]
    distance_series_14 = _daily_distance_series(run_distance_acts, today, days=14)
    ctl_delta_14 = round(ctl_series_14[-1]["value"] - ctl_series_14[0]["value"], 1) if ctl_series_14 else 0.0
    atl_recent_delta = round(atl_series_14[-1]["value"] - atl_series_14[max(0, len(atl_series_14) - 4)]["value"], 1) if atl_series_14 else 0.0
    atl_spike = load_model["atl_today"] > (load_model["ctl_today"] + 8.0) or atl_recent_delta >= 6.0

    provisional_longest_run = max((r["distance_km"] for r in runs_8w), default=0.0)
    desired_peak = _baseline_weekly_goal(goal_ctx["goal_seconds"])
    established_runner = prior_avg >= (desired_peak * 0.35) or consistent_weeks >= 3 or provisional_longest_run >= 18.0
    if phase == "recovery" and not established_runner:
        phase = base_phase
        week_type = "Endurance Build"
    phase_cap_factor = {"base": 0.78, "build": 0.92, "peak": 1.0, "taper": 0.55, "recovery": 0.72, "rebuild": 0.6}.get(phase, 0.9)
    if rebuild_mode:
        weekly_goal_km = round(max(18.0, min(desired_peak * 0.6, max(22.0, prior_avg * 0.65 if prior_avg else 22.0))), 1)
    elif phase == "recovery":
        weekly_goal_km = round(max(24.0, min(desired_peak * 0.82, max(_phase_goal_floor(desired_peak, "recovery"), prior_avg * 0.90 if prior_avg else _phase_goal_floor(desired_peak, "recovery")))), 1)
    elif prior_avg > 0:
        ramp_factor = 1.08 if phase == "base" else 1.10 if phase == "build" else 1.10 if phase == "peak" else 0.7
        weekly_goal_km = round(max(20.0, min(desired_peak * phase_cap_factor, prior_avg * ramp_factor)), 1)
    else:
        weekly_goal_km = round(max(24.0, desired_peak * 0.55), 1)
    if established_runner and phase in {"base", "build", "peak"}:
        weekly_goal_km = max(weekly_goal_km, _phase_goal_floor(desired_peak, phase))
    elif not rebuild_mode and phase in {"base", "build", "peak"}:
        weekly_goal_km = max(weekly_goal_km, _phase_minimum_goal(desired_peak, phase))
    completed_km = rolling_week_distance_km
    remaining_km = round(max(0.0, weekly_goal_km - completed_km), 1)

    runs_30d = [r for r in runs if r["date"] >= today - timedelta(days=30)]
    longest_run = max(runs_8w, key=lambda x: x["distance_km"], default=None)
    latest_long_run = max(runs_30d, key=lambda x: x["distance_km"], default=None)
    long_run_state = _long_run_progress_state(runs, today)
    race_sim_runs = _race_simulation_runs(runs_8w, goal_ctx["goal_seconds"] / 42.195)
    long_run_km = longest_run["distance_km"] if longest_run else 0.0
    max_safe_run = min(long_run_km * 1.1 if long_run_km > 0 else weekly_goal_km * 0.35, weekly_goal_km * 0.35)
    max_safe_run = round(max(5.0, max_safe_run), 1)

    days_remaining_in_week = 6 - today.weekday()
    today_activity_logged = any(a["date"] == today for a in run_distance_acts)
    week_closed = datetime.now(_resolve_timezone(user_timezone)).date() > week_end
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
    adi_status = "Unavailable"
    adi_message = "Need more stable long aerobic running to classify durability."
    pace_medium = (sum(r["pace_sec_per_km"] for r in medium_runs) / len(medium_runs)) if medium_runs else None
    pace_long = (sum(r["pace_sec_per_km"] for r in long_runs) / len(long_runs)) if long_runs else None
    durability_label, pace_drop = _aerobic_durability_from_runs(long_runs)
    if durability_label is not None and pace_drop is not None:
        adi = round(pace_drop * 100.0, 1)
        adi_status = durability_label
        adi_message = f"Estimated late-run fade is {adi:.1f}% based on pace stability and heart-rate drift."

    fri = _fri_from_runs(long_runs)
    if fri is None:
        fri_status = "FRI unavailable"
        fri_message = f"{len(long_runs)} / 3 qualifying long runs completed"
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

    weekly_readiness_target = max(38.0, round(min(55.0, weekly_goal_km * 0.9), 1))
    if phase == "taper":
        weekly_readiness_target = min(weekly_readiness_target, max(25.0, round(prior_avg * 0.65, 1) if prior_avg else 25.0))
    elif phase == "recovery":
        weekly_readiness_target = min(weekly_readiness_target, max(28.0, round(prior_avg * 0.8, 1) if prior_avg else 28.0))
    elif rebuild_mode:
        weekly_readiness_target = min(weekly_readiness_target, max(22.0, round(weekly_goal_km * 0.8, 1)))

    need_medium = max(0, 3 - len(medium_runs))
    need_long = max(0, 2 - len(long_runs))
    need_weeks = max(0, 3 - consistent_weeks)
    if phase == "taper" and consistent_weeks >= 3 and len(long_runs) >= 2:
        need_weekly = 0
    else:
        need_weekly = 0 if rolling_week_distance_km >= weekly_readiness_target else 1

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
            "line3": None if need_weeks == 0 else f"Keep the next {need_weeks} week{'s' if need_weeks > 1 else ''} consistent to build a stable training block.",
            "weight": 0.25,
            "score": min(consistent_weeks / 3.0, 1.0),
        },
        {
            "title": "Weekly mileage",
            "done": rolling_week_distance_km,
            "min": weekly_readiness_target,
            "ready": need_weekly == 0,
            "line1": (
                "Taper mileage handled from prior training"
                if phase == "taper" and need_weekly == 0
                else ("Weekly mileage threshold satisfied" if need_weekly == 0 else "Weekly mileage threshold not met")
            ),
            "line2": (
                f"{rolling_week_distance_km} km completed (taper target: {weekly_readiness_target} km)"
                if phase == "taper"
                else f"{rolling_week_distance_km} km completed (minimum needed: {weekly_readiness_target} km)"
            ),
            "line3": (
                None
                if need_weekly == 0
                else (
                    "Taper week is intentionally lighter. Use prior consistency to protect prediction confidence."
                    if phase == "taper"
                    else f"Build this week toward about {weekly_readiness_target} km total running."
                )
            ),
            "weight": 0.25,
            "score": min(rolling_week_distance_km / max(1.0, weekly_readiness_target), 1.0),
        },
    ]

    readiness_progress = int(round(sum(i["weight"] * i["score"] for i in readiness_items) * 100))
    next_requirement = _readiness_next_action(
        need_long,
        need_medium,
        need_weeks,
        need_weekly,
        weekly_readiness_target,
        rolling_week_distance_km,
        phase,
    )
    if week_closed and rolling_week_distance_km < weekly_readiness_target:
        next_requirement = f"Start next week by rebuilding to about {int(round(weekly_readiness_target))} km and protecting the long run."

    unlock_requirements = []
    if need_long > 0:
        unlock_requirements.append("2 long runs >=18 km")
    if need_weekly > 0:
        unlock_requirements.append(f"{int(round(weekly_readiness_target))} km weekly mileage")
    if need_weeks > 0:
        unlock_requirements.append("3 consistent training weeks")
    if need_medium > 0:
        unlock_requirements.append("3 medium runs between 8-12 km")

    eta_candidates = [need_long, need_weeks, need_weekly]
    if need_medium > 0:
        eta_candidates.append((need_medium + 1) // 2)
    eta_weeks = max(eta_candidates) if eta_candidates else 0
    if eta_weeks <= 0:
        unlock_eta = "Ready now"
    elif eta_weeks == 1:
        unlock_eta = "1 week"
    elif eta_weeks == 2:
        unlock_eta = "1-2 weeks"
    else:
        unlock_eta = f"{eta_weeks} weeks"

    longest_for_shape = latest_long_run["distance_km"] if latest_long_run else 0.0
    consistency_ratio = min(1.0, consistent_weeks / 4.0)
    marathon_shape = min(1.0, (longest_for_shape / 32.0) * 0.4 + (prior_avg / 70.0) * 0.4 + consistency_ratio * 0.2)
    if longest_for_shape < 18:
        bonk_risk_label = "High"
    elif longest_for_shape < 24:
        bonk_risk_label = "Moderate"
    elif longest_for_shape <= 30:
        bonk_risk_label = "Low"
    else:
        bonk_risk_label = "Very Low"
    back_to_back_long_runs = 0
    for idx in range(1, len(long_runs)):
        if (long_runs[idx]["date"] - long_runs[idx - 1]["date"]).days <= 2:
            back_to_back_long_runs += 1
    if back_to_back_long_runs > 0 and bonk_risk_label in {"Low", "Very Low"}:
        bonk_risk_label = "Moderate"
    if load_model["tsb_today"] < -20 and bonk_risk_label in {"Low", "Very Low"}:
        bonk_risk_label = "Moderate"
    bonk_risk_score = 1.0 if bonk_risk_label == "High" else 0.65 if bonk_risk_label == "Moderate" else 0.35 if bonk_risk_label == "Low" else 0.15
    fatigue_flags = {
        "high_fatigue": load_model["tsb_today"] < -20,
        "moderate_fatigue": -20 <= load_model["tsb_today"] < -10,
        "recovered": load_model["tsb_today"] > 0,
        "allow_progression": load_model["tsb_today"] > 5 and not atl_spike,
        "atl_spike": atl_spike,
        "fatigue_ratio": load_model["fatigue_ratio"],
        "tsb": load_model["tsb_today"],
    }

    return {
        "medium_runs": medium_runs,
        "long_runs": long_runs,
        "race_simulation_runs": race_sim_runs,
        "recent_race_runs": recent_race_runs,
        "vo2max_estimate": vo2max_estimate,
        "pace_medium": pace_medium,
        "pace_long": pace_long,
        "marathon_specific_runs": marathon_specific_runs,
        "goal_marathon_pace_sec_per_km": goal_ctx["goal_seconds"] / 42.195,
        "ctl_proxy": ctl_today,
        "atl_proxy": load_model["atl_today"],
        "tsb_proxy": load_model["tsb_today"],
        "fatigue_ratio": load_model["fatigue_ratio"],
        "ctl_series_14": ctl_series_14,
        "atl_series_14": load_model["atl_series"],
        "distance_series_14": distance_series_14,
        "readiness": {"ready": all(i["ready"] for i in readiness_items), "items": readiness_items},
        "readiness_progress_pct": readiness_progress,
        "next_requirement": next_requirement,
        "unlock_requirements": unlock_requirements,
        "unlock_eta": unlock_eta,
        "bonk_risk": {"score": round(bonk_risk_score, 2), "label": bonk_risk_label, "marathon_shape": round(marathon_shape, 2)},
        "weekly": {
            "weekly_goal_km": weekly_goal_km,
            "completed_km": completed_km,
            "cross_training_km": cross_training_distance_km,
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
            "weekly_mileage_change": round((rolling_weeks[0] if len(rolling_weeks) > 0 else 0.0) - (rolling_weeks[1] if len(rolling_weeks) > 1 else 0.0), 1),
            "prior_avg_km": prior_avg,
            "phase": phase,
            "base_phase": base_phase,
            "display_phase": base_phase,
            "week_type": week_type,
            "cycle_week": cycle_week,
            "rebuild_mode": rebuild_mode,
            "weekly_readiness_target_km": weekly_readiness_target,
            "fatigue_ratio": load_model["fatigue_ratio"],
            "ctl_delta_14": ctl_delta_14,
            "atl_spike": atl_spike,
            "allow_progression": fatigue_flags["allow_progression"],
            "training_consistency_ratio": round(training_consistency_ratio, 2),
            "race_date": goal_ctx["race_date"],
            "race_distance_km": goal_ctx["distance_km"],
            "weeks_to_race": max(0.0, goal_ctx["days_remaining"] / 7.0),
            "high_fatigue": fatigue_flags["high_fatigue"],
            "moderate_fatigue": fatigue_flags["moderate_fatigue"],
            "long_run_failed_recent": long_run_state.get("failed_recent", False),
            "goal_marathon_pace_sec_per_km": goal_ctx["goal_seconds"] / 42.195,
            "ctl_proxy": ctl_today,
        },
        "longest_run": longest_run,
        "latest_long_run": latest_long_run,
        "long_run_state": long_run_state,
        "phase": phase,
        "base_phase": base_phase,
        "week_type": week_type,
        "cycle_week": cycle_week,
        "gap_days": gap_days,
        "max_gap_days": gap_metrics["max_gap_days"],
        "rebuild_mode": rebuild_mode,
        "back_to_back_long_runs": back_to_back_long_runs,
        "fatigue_flags": fatigue_flags,
        "endurance": {
            "lrr": lrr,
            "lrr_status": lrr_status,
            "lrr_color": _status_color(lrr_status),
            "lrr_warning": lrr_warning,
            "lrr_message": lrr_message,
            "adi": adi,
            "adi_status": adi_status,
            "adi_color": _status_color(adi_status),
            "adi_message": adi_message,
            "fri": fri,
            "fri_status": fri_status,
            "fri_color": _status_color(fri_status),
            "fri_message": fri_message,
        },
        "personal_best_fm_seconds": _safe_pb_seconds(goal_ctx.get("personal_best"), goal_ctx["distance_km"]),
        "pb_hm_seconds":  _parse_time_str(goal_ctx.get("pb_hm")),
        "pb_10k_seconds": _parse_time_str(goal_ctx.get("pb_10k")),
        "pb_5k_seconds":  _parse_time_str(goal_ctx.get("pb_5k")),
        "best_pb_vdot":   _best_pb_vdot(goal_ctx),
        "goal": {
            "goal_seconds": goal_ctx["goal_seconds"],
            "distance_km": goal_ctx["distance_km"],
            "goal_time": goal_ctx["goal_time"],
        },
    }
def _marathon_prediction_seconds(metrics):
    return service_marathon_prediction_seconds(metrics)


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
    new_flat = _marathon_prediction_seconds(metrics)
    if not new_flat:
        longest = metrics.get("longest_run")
        pace_long = metrics.get("pace_long")
        pace_medium = metrics.get("pace_medium")
        if pace_long:
            new_flat = pace_long * 42.195 * 1.10
        elif pace_medium:
            new_flat = pace_medium * 42.195 * 1.14
        elif longest and longest.get("pace_sec_per_km"):
            new_flat = longest["pace_sec_per_km"] * 42.195 * 1.16
        else:
            return {
                "valid": False,
                "current_projection": "--",
                "race_day_projection": "--",
                "current_projection_range": "--",
                "race_day_projection_range": "--",
                "gap_to_goal_range": "--",
                "probability": None,
                "gap_to_goal": "--",
                "goal_alignment": "Too early to compare",
                "prediction_confidence": "Low",
                "prediction_confidence_score": 0.0,
                "goal_progress_pct": 0,
                "note": "Not enough reliable running data for a trustworthy marathon projection yet.",
                "goal_comparison": "Too early to compare to your goal",
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
    long_run_count = len(metrics.get("long_runs", []))
    medium_count = len(metrics.get("medium_runs", []))
    marathon_specific_count = len(metrics.get("marathon_specific_runs", []))
    race_sim_count = len(metrics.get("race_simulation_runs", []))
    race_count = len(metrics.get("recent_race_runs", []))
    consistency_count = next((i["done"] for i in metrics["readiness"]["items"] if i["title"] == "Consistent mileage weeks"), 0)
    confidence_score = min(
        1.0,
        (0.28 * min(long_run_count / 3.0, 1.0))
        + (0.24 * min(marathon_specific_count / 3.0, 1.0))
        + (0.18 * min(race_sim_count, 1.0))
        + (0.15 * min(race_count, 1.0))
        + (0.10 * min(consistency_count / 3.0, 1.0))
        + (0.05 * min(medium_count / 4.0, 1.0)),
    )
    range_width = max(0.025, min(0.08, 0.08 - (0.045 * confidence_score)))
    flat_low = flat * (1.0 - range_width)
    flat_high = flat * (1.0 + range_width)
    race_low = race_proj * (1.0 - range_width)
    race_high = race_proj * (1.0 + range_width)
    gap = race_proj - goal_ctx["goal_seconds"]
    gap_low = race_low - goal_ctx["goal_seconds"]
    gap_high = race_high - goal_ctx["goal_seconds"]
    has_marathon_specific_base = len(metrics.get("long_runs", [])) >= 1
    provisional_prediction = not has_marathon_specific_base

    prediction_confidence = _confidence_label(confidence_score)
    probability = _goal_probability(race_proj, goal_ctx["goal_seconds"]) if metrics["readiness"]["ready"] and not provisional_prediction else None
    goal_alignment = _goal_alignment_label(probability)
    if provisional_prediction:
        goal_comparison = "Too early to compare to your goal"
    elif probability is None:
        goal_comparison = "Need a few more marathon-specific signals"
    elif probability >= 75:
        goal_comparison = "Goal is on track"
    elif probability >= 45:
        goal_comparison = "Goal is within reach"
    elif probability >= 20:
        goal_comparison = "Goal is building"
    else:
        goal_comparison = "Goal is still a stretch"

    return {
        "valid": metrics["readiness"]["ready"] and not provisional_prediction,
        "current_projection": _fmt_hms(flat),
        "race_day_projection": _fmt_hms(race_proj),
        "current_projection_range": _fmt_range(flat_low, flat_high),
        "race_day_projection_range": _fmt_range(race_low, race_high),
        "gap_to_goal_range": "--" if provisional_prediction else _fmt_minutes_range(gap_low, gap_high),
        "probability": probability,
        "gap_to_goal": _fmt_gap(gap),
        "goal_alignment": goal_alignment,
        "goal_comparison": goal_comparison,
        "prediction_confidence": prediction_confidence,
        "prediction_confidence_score": round(confidence_score, 2),
        "goal_progress_pct": int(max(0, min(100, (goal_ctx["goal_seconds"] / race_proj) * 100))),
        "note": (
            f"Built from { _projection_basis_summary(metrics) }."
            if metrics["readiness"]["ready"] and not provisional_prediction
            else (
                f"Early estimate only. Built from { _projection_basis_summary(metrics) }. Add a qualifying long run to compare reliably with your goal."
                if provisional_prediction
                else (metrics["next_requirement"] or "Range shown with limited training signals. Complete key readiness sessions.")
            )
        ),
    }
def performance_intelligence(user_id, user_timezone=None):
    goal = get_goal(user_id)
    if not goal:
        return None

    today_local = _today_local(user_timezone)
    goal_ctx = build_goal_context(goal, today_local=today_local)
    metrics = _metrics_layer(user_id, goal_ctx, user_timezone=user_timezone)
    prediction = _prediction_layer(user_id, goal_ctx, metrics)

    # Per-distance predictions via VDOT anchor (5K/10K/HM/FM).
    # Uses PB_FLOORS as a guaranteed floor so predictions are never
    # dragged below demonstrated fitness by sparse Strava data.
    all_distances = service_predict_all_distances(metrics, today_local)

    # Wall analysis — marathon only
    wall_analysis = None
    if goal_ctx["distance_km"] >= 40.0:
        wall_analysis = service_marathon_wall_analysis(metrics)

    if goal_ctx["distance_km"] <= 10:
        target_ctl = 45
    elif goal_ctx["distance_km"] <= 21.1:
        target_ctl = 55
    else:
        target_ctl = 62

    weekly = metrics["weekly"]
    longest = metrics["longest_run"]
    long_run_state = metrics.get("long_run_state", {})

    training_status = {
        "title": "Training Status",
        "summary": "Projection is ready for goal comparison." if prediction["valid"] else "Projection is still building.",
        "detail": prediction["note"],
        "progress_pct": metrics["readiness_progress_pct"],
        "next_requirement": metrics["next_requirement"],
    }

    ctl_progress_pct = int(max(0, min(100, (metrics["ctl_proxy"] / max(1.0, target_ctl)) * 100)))

    ctl_series_14 = metrics["ctl_series_14"]
    ctl_delta = round(ctl_series_14[-1]["value"] - ctl_series_14[0]["value"], 1) if ctl_series_14 else 0.0
    if ctl_delta > 2.0:
        ctl_trend_text = f"Your 6-week fitness load is rising by {ctl_delta:.1f} over the last 14 days."
        fitness_trend_label = "Building"
    elif ctl_delta < -2.0:
        ctl_trend_text = f"Your 6-week fitness load has eased by {abs(ctl_delta):.1f} over the last 14 days."
        fitness_trend_label = "Easing"
    else:
        ctl_trend_text = "Your 6-week fitness load is holding steady."
        fitness_trend_label = "Steady"

    atl_series = metrics["atl_series_14"]
    recent_load_delta = round(atl_series[-1]["value"] - atl_series[0]["value"], 1) if atl_series else 0.0
    momentum_score = recent_load_delta
    if momentum_score >= 4:
        training_momentum = "Building"
    elif momentum_score <= -4:
        training_momentum = "Easing"
    else:
        training_momentum = "Steady"

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

    consistent_weeks = next((i["done"] for i in metrics["readiness"]["items"] if i["title"] == "Consistent mileage weeks"), 0)
    consistency_ratio = float(metrics["weekly"].get("training_consistency_ratio") or 0.0)
    consistency_pct = int(round(consistency_ratio * 100))
    tsb_today = round(metrics.get("tsb_proxy", 0.0), 1)
    long_run_progress = min(1.0, (longest["distance_km"] / 32.0)) if longest else 0.0
    weekly_mileage_progress = min(1.0, weekly["completed_km"] / max(1.0, weekly.get("weekly_readiness_target_km") or 45.0))
    consistency_progress = min(1.0, consistency_ratio)
    fitness_progress = min(1.0, round(metrics["ctl_proxy"], 1) / max(1.0, target_ctl))
    specificity_progress = min(
        1.0,
        (0.6 * min(len(metrics.get("marathon_specific_runs", [])) / 3.0, 1.0))
        + (0.4 * min(len(metrics.get("race_simulation_runs", [])), 1.0)),
    )
    if tsb_today < -20:
        fatigue_control_progress = 0.2
    elif tsb_today < -10:
        fatigue_control_progress = 0.5
    elif tsb_today <= 5:
        fatigue_control_progress = 0.8
    else:
        fatigue_control_progress = 1.0

    marathon_readiness_pct = int(round((
        0.28 * long_run_progress
        + 0.22 * weekly_mileage_progress
        + 0.16 * consistency_progress
        + 0.14 * fitness_progress
        + 0.12 * specificity_progress
        + 0.08 * fatigue_control_progress
    ) * 100))
    if marathon_readiness_pct >= 75:
        readiness_status = "Strong"
    elif marathon_readiness_pct >= 45:
        readiness_status = "Building"
    else:
        readiness_status = "Early Build"

    next_step = metrics["next_requirement"] or "Maintain this week and protect the scheduled long run."
    fatigue_balance, fatigue_balance_note = _freshness_label(tsb_today)

    if longest and longest["distance_km"] >= 28:
        long_run_depth = "Strong"
        long_run_note = f"Your longest recent run is {round(longest['distance_km'], 1)} km, which is strong marathon-specific depth."
    elif longest and longest["distance_km"] >= 21:
        long_run_depth = "Building"
        long_run_note = f"Your longest recent run is {round(longest['distance_km'], 1)} km. The next big step is building toward 24-30 km."
    elif longest:
        long_run_depth = "Early"
        long_run_note = f"Your longest recent run is {round(longest['distance_km'], 1)} km. Keep progressing the weekly long run before worrying about pace."
    else:
        long_run_depth = "Not started"
        long_run_note = "No qualifying long run yet. Start by locking in a consistent weekly long run."

    durability_display = metrics["endurance"]["adi_status"]
    durability_note = metrics["endurance"]["adi_message"]
    if durability_display == "Unavailable":
        durability_display = "Building"
        durability_note = "We need a few more steady long runs to judge how well you hold pace late in the run."

    fueling_risk = metrics["bonk_risk"]["label"]
    if longest and longest["distance_km"] < 24:
        fueling_note = "Keep extending the long run and practice fueling on every run over 90 minutes."
    else:
        fueling_note = "Keep fueling practice consistent on long runs so race-day pace stays sustainable."

    return {
        "goal": goal_ctx,
        "goal_time_display": goal_ctx["goal_time"],
        "goal_milestone_label": _goal_milestone_label(goal_ctx["goal_seconds"], goal_ctx["distance_km"]),
        "current_projection": prediction["current_projection"],
        "race_day_projection": prediction["race_day_projection"],
        "current_projection_range": prediction["current_projection_range"],
        "race_day_projection_range": prediction["race_day_projection_range"],
        "probability": prediction["probability"],
        "gap_to_goal": prediction["gap_to_goal"],
        "gap_to_goal_range": prediction["gap_to_goal_range"],
        "goal_alignment": prediction["goal_alignment"],
        "goal_comparison": prediction["goal_comparison"],
        "prediction_confidence": prediction["prediction_confidence"],
        "prediction_confidence_score": prediction["prediction_confidence_score"],
        "prediction_confidence_pct": int(round(prediction["prediction_confidence_score"] * 100)),
        "prediction_note": prediction["note"],
        "goal_progress_pct": prediction["goal_progress_pct"],
        "insufficient_data": not prediction["valid"],
        "current_ctl": round(metrics["ctl_proxy"], 1),
        "current_atl": round(metrics.get("atl_proxy", 0.0), 1),
        "current_tsb": tsb_today,
        "target_ctl": target_ctl,
        "ctl_progress_pct": ctl_progress_pct,
        "ctl_trend_text": ctl_trend_text,
        "fitness_trend_label": fitness_trend_label,
        "fitness_load_label": f"6-week fitness load {int(round(metrics['ctl_proxy']))}",
        "recent_load_label": f"7-day load {int(round(metrics.get('atl_proxy', 0.0)))}",
        "freshness_label": f"Freshness {tsb_today:+.1f}",
        "load_terms_label": f"Advanced: CTL {round(metrics['ctl_proxy'], 1)} / ATL {round(metrics.get('atl_proxy', 0.0), 1)} / TSB {tsb_today:+.1f}",
        "training_consistency_label": f"{consistency_pct}%",
        "fatigue_balance": fatigue_balance,
        "fatigue_balance_note": fatigue_balance_note,
        "training_momentum": training_momentum,
        "training_momentum_score": momentum_score,
        "weekly": weekly,
        "prediction_readiness": metrics["readiness"],
        "unlock_requirements": metrics["unlock_requirements"],
        "unlock_eta": metrics["unlock_eta"],
        "endurance": metrics["endurance"],
        "bonk_risk": metrics["bonk_risk"],
        "fatigue_flags": metrics["fatigue_flags"],
        "training_status": training_status,
        "training_counts": {
            "long_runs": len(metrics["long_runs"]),
            "medium_runs": len(metrics["medium_runs"]),
            "consistent_weeks": consistent_weeks,
        },
        "race_readiness": race_readiness,
        "marathon_readiness_pct": marathon_readiness_pct,
        "marathon_readiness_status": readiness_status,
        "marathon_readiness_next_step": next_step,
        "marathon_specificity_pct": int(round(specificity_progress * 100)),
        "fatigue_control_pct": int(round(fatigue_control_progress * 100)),
        "endurance_profile": {
            "long_run_depth": long_run_depth,
            "long_run_note": long_run_note,
            "durability": durability_display,
            "durability_note": durability_note,
            "fueling_risk": fueling_risk,
            "fueling_note": fueling_note,
        },
        "charts": {
            "weekly_distance_14": metrics["distance_series_14"],
            "ctl_14": ctl_series_14,
        },
        "long_run": {
            "longest_km": round(longest["distance_km"], 1) if longest else 0.0,
            "longest_date": longest["date"].isoformat() if longest else None,
            "latest_km": round(metrics["latest_long_run"]["distance_km"], 1) if metrics.get("latest_long_run") else 0.0,
            "latest_date": metrics["latest_long_run"]["date"].isoformat() if metrics.get("latest_long_run") else None,
            "next_milestone_km": long_run_state.get("next_step", 24),
            "milestones_completed": long_run_state.get("milestones_completed", 0),
            "failed_recent": long_run_state.get("failed_recent", False),
            "progress": min(100, int((weekly["completed_km"] / max(1.0, weekly["weekly_goal_km"])) * 100)),
        },
        "wall_analysis": wall_analysis,
        "all_distances": all_distances,
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

