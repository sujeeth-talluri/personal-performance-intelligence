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
        "moderate": 1.02,
        "hilly": 1.04,
        "mountain": 1.08,
    }

    return {
        "event_name": goal_row["race_name"],
        "distance_km": float(goal_row["race_distance"]),
        "goal_time": goal_row["goal_time"],
        "goal_seconds": goal_seconds,
        "race_date": goal_row["race_date"],
        "days_remaining": days_remaining,
        "target_pace": f"{int(pace_sec // 60)}:{int(pace_sec % 60):02d} / km",
        "elevation_factor": elevation_map.get(goal_row["elevation_type"], 1.0),
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

        # Remove race-like efforts/outliers: ultra long or near-race simulation runs.
        if distance >= max(35, goal_distance * 0.8):
            continue
        if distance >= min(goal_distance * 0.75, 32):
            continue

        # Keep only useful training runs for projection.
        if distance < 6 or distance > min(32, goal_distance * 0.7):
            continue

        # Exclude very aggressive effort that likely reflects race day, not training baseline.
        if distance >= 10 and pace < 220:
            continue

        runs.append({
            "date": run_date,
            "distance": distance,
            "duration": duration,
            "pace": pace,
            "ctl": float(row["ctl"] or 0),
        })

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

    training_runs = _recent_training_runs(rows, goal_distance, lookback_days=56)
    if not training_runs:
        return None

    # Weighted by distance so a strong 18 km run matters more than an 8 km run.
    training_runs.sort(key=lambda r: r["distance"], reverse=True)
    anchors = training_runs[:6]

    weighted_distance = sum(r["distance"] for r in anchors)
    weighted_pace = sum(r["pace"] * r["distance"] for r in anchors) / weighted_distance
    longest_recent = max(r["distance"] for r in anchors)

    long_ratio = longest_recent / goal_distance if goal_distance else 0
    if long_ratio < 0.4:
        endurance_penalty = 0.18
    elif long_ratio < 0.5:
        endurance_penalty = 0.14
    elif long_ratio < 0.6:
        endurance_penalty = 0.10
    elif long_ratio < 0.7:
        endurance_penalty = 0.07
    else:
        endurance_penalty = 0.04

    current_projection_sec = weighted_pace * goal_distance * (1 + endurance_penalty)

    if goal_distance <= 10:
        target_ctl = 45
    elif goal_distance <= 21.1:
        target_ctl = 55
    elif goal_distance <= 42.2:
        target_ctl = 62
    else:
        target_ctl = 75

    ctl_ratio = min(1.25, max(0.3, current_ctl / target_ctl if target_ctl else 1.0))
    weeks_remaining = days_remaining / 7.0
    ctl_gap_ratio = max(0.0, (target_ctl - current_ctl) / target_ctl)

    # Improvement can happen if weeks remain and training load can still build.
    potential_improvement = min(0.12, (weeks_remaining * 0.008) + (ctl_gap_ratio * 0.05))
    if weeks_remaining < 2:
        potential_improvement *= 0.3

    race_day_projection_sec = current_projection_sec * (1 - potential_improvement)
    adjusted_projection = race_day_projection_sec * goal_context["elevation_factor"]

    flat_gap_sec = race_day_projection_sec - goal_seconds
    adjusted_gap_sec = adjusted_projection - goal_seconds

    consistency = min(1.0, len(training_runs) / 16.0)
    pace_score = max(0.0, 1.0 - max(0.0, flat_gap_sec) / (goal_seconds * 0.25))
    ctl_score = min(1.0, ctl_ratio)

    probability = int(10 + (pace_score * 50) + (ctl_score * 25) + (consistency * 15))
    probability = max(5, min(probability, 95))

    return {
        "current_projection": _fmt_hms(current_projection_sec),
        "race_day_projection": _fmt_hms(race_day_projection_sec),
        "elevation_adjusted": _fmt_hms(adjusted_projection),
        "probability": probability,
        "flat_gap": _fmt_gap(flat_gap_sec),
        "adjusted_gap": _fmt_gap(adjusted_gap_sec),
        "current_ctl": round(current_ctl, 1),
        "target_ctl": target_ctl,
        "ctl_description": "CTL is your training fitness score from recent weeks. Higher CTL usually means better endurance base.",
    }


def long_run_progress(user_id):
    rows = fetch_all_metrics(user_id)
    today = date.today()
    since = today - timedelta(days=56)

    recent_runs = []
    for row in rows:
        run_date = _parse_timestamp(row["timestamp"]).date()
        if run_date < since:
            continue

        distance = float(row["distance_km"] or 0)
        if distance <= 0 or distance >= 35:
            continue

        recent_runs.append((distance, run_date))

    if not recent_runs:
        return {
            "longest_run_km": 0.0,
            "longest_run_date": None,
            "next_milestone": 8,
            "progress_pct": 0,
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
        load_risk = "High"
    elif latest_tsb < -10:
        load_risk = "Moderate"
    else:
        load_risk = "Low"

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
        return {"title": "Aerobic Run", "details": "8-12 km easy", "purpose": "Build durability"}
    if p < 60:
        return {"title": "Long Run", "details": "14-20 km steady aerobic", "purpose": "Improve endurance"}
    if p < 75:
        return {"title": "Tempo Session", "details": "5-8 km threshold", "purpose": "Raise race pace"}
    return {"title": "Race Specific", "details": "Marathon-pace intervals", "purpose": "Sharpen race form"}


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
