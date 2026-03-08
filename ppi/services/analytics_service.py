from datetime import date, datetime, timedelta

from ..repositories import fetch_all_metrics, fetch_latest_metric, fetch_recent_metrics

THRESHOLD_HR = 168


def _parse_timestamp(timestamp):
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")


def _fmt_hms(total_seconds):
    seconds = max(0, int(total_seconds))
    return f"{seconds // 3600}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def _fmt_gap(total_seconds):
    sign = "+" if total_seconds > 0 else "-"
    seconds = abs(int(total_seconds))
    return f"{sign}{seconds // 60}:{seconds % 60:02d}"


def compute_stress(activity):
    duration_min = activity.get("moving_time", 0) / 60
    avg_hr = activity.get("average_heartrate")
    if not avg_hr or duration_min <= 0:
        return 0.0

    intensity_factor = avg_hr / THRESHOLD_HR
    return round(duration_min * intensity_factor, 2)


def update_training_load(stress_by_date, starting_atl=0.0, starting_ctl=0.0):
    if not stress_by_date:
        return {}

    atl = float(starting_atl)
    ctl = float(starting_ctl)
    metrics = {}

    all_days = sorted(stress_by_date)
    cursor_day = all_days[0]
    end_day = all_days[-1]

    while cursor_day <= end_day:
        stress = float(stress_by_date.get(cursor_day, 0.0))
        atl = atl + (stress - atl) * (1 / 7)
        ctl = ctl + (stress - ctl) * (1 / 42)
        tsb = ctl - atl

        metrics[cursor_day] = {
            "stress": round(stress, 2),
            "atl": round(atl, 2),
            "ctl": round(ctl, 2),
            "tsb": round(tsb, 2),
        }
        cursor_day += timedelta(days=1)

    return metrics


def calculate_readiness(tsb, resting_hr=48):
    score = 70
    if tsb < -20:
        score = 35
    elif -20 <= tsb < -10:
        score = 50
    elif -10 <= tsb <= 5:
        score = 70
    elif 5 < tsb <= 15:
        score = 85

    deviation = resting_hr - 48
    if deviation >= 5:
        score -= 15
    elif deviation >= 2:
        score -= 7

    return max(0, min(score, 100))


def build_goal_context(goal_row):
    if not goal_row:
        return None

    race_date_obj = datetime.strptime(goal_row["race_date"], "%Y-%m-%d").date()
    days_remaining = (race_date_obj - date.today()).days

    h, m, s = map(int, goal_row["goal_time"].split(":"))
    goal_seconds = h * 3600 + m * 60 + s
    pace_sec = goal_seconds / float(goal_row["race_distance"])

    elevation_map = {
        "flat": 1.00,
        "moderate": 1.03,
        "hilly": 1.06,
        "mountain": 1.10,
    }

    elevation_type = goal_row["elevation_type"]

    return {
        "event_name": goal_row["race_name"],
        "distance_km": float(goal_row["race_distance"]),
        "goal_time": goal_row["goal_time"],
        "goal_seconds": goal_seconds,
        "race_date": goal_row["race_date"],
        "days_remaining": days_remaining,
        "target_pace": f"{int(pace_sec // 60)}:{int(pace_sec % 60):02d} / km",
        "elevation_factor": elevation_map.get(elevation_type, 1.02),
        "elevation_type": elevation_type,
        "race_projection_label": f"Race Day Projection ({goal_row['race_name']} Course)",
    }


def _recent_training_runs(rows, goal_distance, lookback_days=56):
    today = date.today()
    since = today - timedelta(days=lookback_days)

    runs = []
    for row in rows:
        run_date = _parse_timestamp(row["timestamp"]).date()
        if run_date < since:
            continue

        distance = float(row["distance_km"] or 0)
        duration = float(row["moving_time_sec"] or 0)
        if distance <= 0 or duration <= 0:
            continue

        pace = duration / distance

        # Keep only meaningful training runs and exclude likely race efforts/outliers.
        if distance < 5 or distance >= 42.2:
            continue
        if distance >= max(35, goal_distance * 0.8):
            continue
        if distance >= 10 and pace < 220:
            continue

        runs.append(
            {
                "date": run_date,
                "distance": distance,
                "duration": duration,
                "pace": pace,
                "ctl": float(row["ctl"] or 0),
            }
        )

    return runs


def performance_intelligence(user_id, goal_context):
    if not goal_context:
        return None

    rows = fetch_all_metrics(user_id)
    latest = fetch_latest_metric(user_id)
    if not rows or not latest:
        return None

    goal_distance = goal_context["distance_km"]
    goal_seconds = goal_context["goal_seconds"]
    days_remaining = max(0, int(goal_context["days_remaining"]))

    current_ctl = float(latest["ctl"] or 0)

    recent_runs = _recent_training_runs(rows, goal_distance, lookback_days=56)
    if len(recent_runs) < 2:
        return None

    recent_runs.sort(key=lambda r: r["distance"], reverse=True)

    long_run_candidates = [r for r in recent_runs if r["distance"] >= max(10, goal_distance * 0.35)]
    long_run_pace = (
        sum(r["pace"] for r in long_run_candidates[:3]) / len(long_run_candidates[:3])
        if long_run_candidates
        else sum(r["pace"] for r in recent_runs[:3]) / len(recent_runs[:3])
    )

    average_run_pace = sum(r["pace"] for r in recent_runs) / len(recent_runs)

    tempo_candidates = [r for r in recent_runs if 8 <= r["distance"] <= 16]
    tempo_estimate = (
        sum(r["pace"] for r in tempo_candidates[:5]) / len(tempo_candidates[:5])
        if tempo_candidates
        else average_run_pace * 0.97
    )

    base_pace = (0.4 * long_run_pace) + (0.3 * average_run_pace) + (0.3 * tempo_estimate)

    longest_recent = max(r["distance"] for r in recent_runs)
    distance_readiness = min(1.0, longest_recent / max(goal_distance * 0.65, 12))

    if goal_distance <= 10:
        target_ctl = 45
    elif goal_distance <= 21.1:
        target_ctl = 55
    elif goal_distance <= 42.2:
        target_ctl = 62
    else:
        target_ctl = 75

    ctl_ratio = min(1.25, max(0.35, current_ctl / target_ctl if target_ctl else 1.0))

    current_projection_flat_sec = base_pace * goal_distance * (1 + (1 - distance_readiness) * 0.08)

    weeks_remaining = days_remaining / 7.0
    improvement = min(0.12, (weeks_remaining * 0.006) + (max(0.0, 1 - ctl_ratio) * 0.05))
    if weeks_remaining < 2:
        improvement *= 0.3

    race_day_flat_sec = current_projection_flat_sec * (1 - improvement)

    # Race-day projection must respect selected race course context.
    race_day_course_sec = race_day_flat_sec * max(1.0, goal_context["elevation_factor"])

    # Monotonic course rule: flat <= moderate <= hilly
    if race_day_course_sec < race_day_flat_sec:
        race_day_course_sec = race_day_flat_sec

    gap_sec = race_day_course_sec - goal_seconds

    weekly_avg_km = 0.0
    if recent_runs:
        window_days = 56
        total_km = sum(r["distance"] for r in recent_runs)
        weekly_avg_km = (total_km / window_days) * 7

    consistency = min(1.0, len(recent_runs) / 24.0)
    pace_score = max(0.0, 1.0 - max(0.0, gap_sec) / (goal_seconds * 0.22))
    ctl_score = min(1.0, ctl_ratio)

    probability = int((pace_score * 55) + (ctl_score * 25) + (consistency * 20))
    probability = max(5, min(probability, 95))

    return {
        "current_projection": _fmt_hms(current_projection_flat_sec),
        "race_day_projection": _fmt_hms(race_day_course_sec),
        "probability": probability,
        "gap_to_goal": _fmt_gap(gap_sec),
        "current_ctl": round(current_ctl, 1),
        "target_ctl": target_ctl,
        "ctl_description": "CTL (Chronic Training Load) represents your long-term training fitness based on recent training stress. Higher CTL usually indicates stronger endurance capacity.",
        "weekly_avg_km": round(weekly_avg_km, 1),
    }


def long_run_progress(user_id):
    rows = fetch_all_metrics(user_id)
    today = date.today()
    recent_since = today - timedelta(days=56)
    durability_since = today - timedelta(days=112)

    recent_runs = []
    durability_runs = []

    for row in rows:
        run_date = _parse_timestamp(row["timestamp"]).date()
        distance = float(row["distance_km"] or 0)
        if distance <= 0 or distance >= 42.2:
            continue

        if run_date >= recent_since:
            recent_runs.append((distance, run_date))
        if run_date >= durability_since:
            durability_runs.append((distance, run_date))

    if not recent_runs:
        return {
            "longest_run_km": 0.0,
            "longest_run_date": None,
            "next_milestone": 8,
            "progress_pct": 0,
            "durability_longest_km": round(max([d for d, _ in durability_runs], default=0.0), 1),
        }

    longest_run_km, longest_run_date = max(recent_runs, key=lambda x: x[0])

    milestones = [8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30]
    next_milestone = 30
    for m in milestones:
        if longest_run_km < m:
            next_milestone = m
            break

    pct = int((longest_run_km / next_milestone) * 100) if next_milestone else 0

    return {
        "longest_run_km": round(longest_run_km, 1),
        "longest_run_date": longest_run_date.isoformat(),
        "next_milestone": next_milestone,
        "progress_pct": min(pct, 100),
        "durability_longest_km": round(max([d for d, _ in durability_runs], default=0.0), 1),
    }


def weekly_training(user_id):
    rows = fetch_all_metrics(user_id)
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)

    week_rows = []
    for row in rows:
        d = _parse_timestamp(row["timestamp"]).date()
        if week_start <= d <= week_end:
            week_rows.append(row)

    weekly_km = round(sum(float(row["distance_km"] or 0) for row in week_rows), 1)

    latest = rows[-1] if rows else None
    latest_ctl = float(latest["ctl"] or 0) if latest else 0

    ctl_7d_ago = latest_ctl
    for row in rows:
        d = _parse_timestamp(row["timestamp"]).date()
        if d <= today - timedelta(days=7):
            ctl_7d_ago = float(row["ctl"] or 0)
    ctl_trend = round(latest_ctl - ctl_7d_ago, 1)

    latest_tsb = float(latest["tsb"] or 0) if latest else 0
    if latest_tsb < -20:
        load_risk = "Red"
    elif latest_tsb < -10:
        load_risk = "Yellow"
    else:
        load_risk = "Green"

    activity_done_today = False
    if latest:
        activity_done_today = _parse_timestamp(latest["timestamp"]).date() == today

    return {
        "weekly_distance": weekly_km,
        "ctl_trend": ctl_trend,
        "load_risk": load_risk,
        "activity_done_today": activity_done_today,
        "week_label": f"{week_start.isoformat()} to {week_end.isoformat()}",
    }


def today_training(intel):
    if not intel:
        return {"title": "Easy Run", "details": "30-40 min easy", "purpose": "Build consistency"}

    p = intel["probability"]
    if p < 40:
        return {"title": "Aerobic Run", "details": "12-16 km easy", "purpose": "Build aerobic endurance and durability."}
    if p < 60:
        return {"title": "Long Run", "details": "16-22 km steady", "purpose": "Improve endurance and long-run confidence."}
    if p < 75:
        return {"title": "Tempo Session", "details": "6-10 km comfortably hard", "purpose": "Improve sustained race pace."}
    return {"title": "Race Specific", "details": "Marathon pace segments", "purpose": "Sharpen race execution."}


def recent_runs(user_id, limit=5):
    rows = fetch_recent_metrics(user_id, limit=limit)
    out = []
    for row in rows:
        out.append(
            {
                "date": _parse_timestamp(row["timestamp"]).date().isoformat(),
                "distance": round(float(row["distance_km"] or 0), 1),
                "time": _fmt_hms(float(row["moving_time_sec"] or 0)),
                "hr": int(float(row["avg_hr"])) if row["avg_hr"] else None,
            }
        )
    return out
