from datetime import date, datetime, timedelta

from ..repositories import fetch_all_metrics, fetch_latest_metric, fetch_recent_metrics

THRESHOLD_HR = 168


def _parse_timestamp(timestamp):
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")


def format_hms(total_seconds):
    seconds = max(0, int(total_seconds))
    return f"{seconds // 3600}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def format_gap(total_seconds):
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
    pace_sec = goal_seconds / float(goal_row["distance_km"])

    elevation_map = {
        "flat": 1.00,
        "moderate": 1.02,
        "hilly": 1.04,
        "mountain": 1.08,
    }

    return {
        "event_name": goal_row["event_name"],
        "distance_km": float(goal_row["distance_km"]),
        "goal_time": goal_row["goal_time"],
        "goal_seconds": goal_seconds,
        "race_date": goal_row["race_date"],
        "days_remaining": days_remaining,
        "target_pace": f"{int(pace_sec // 60)}:{int(pace_sec % 60):02d} / km",
        "elevation_factor": elevation_map.get(goal_row["elevation_type"], 1.0),
    }


def estimate_race_projection(athlete_id, goal_context):
    if not goal_context:
        return None

    rows = fetch_all_metrics(athlete_id)
    last_row = fetch_latest_metric(athlete_id)
    if not rows or not last_row:
        return None

    goal_distance = goal_context["distance_km"]
    current_ctl = float(last_row["ctl"] or 0)
    current_tsb = float(last_row["tsb"] or 0)

    candidates = []
    for row in rows:
        distance = float(row["distance_km"] or 0)
        duration = float(row["moving_time_sec"] or 0)
        if distance <= 0 or duration <= 0:
            continue
        if distance < min(5.0, goal_distance * 0.2):
            continue
        candidates.append((distance, duration))

    if not candidates:
        return None

    candidates.sort(key=lambda entry: entry[0], reverse=True)
    anchor_distance, anchor_duration = candidates[0]

    raw_projection = anchor_duration * (goal_distance / anchor_distance) ** 1.06

    if goal_distance <= 10:
        target_ctl = 45
    elif goal_distance <= 21.1:
        target_ctl = 55
    elif goal_distance <= 42.2:
        target_ctl = 62
    else:
        target_ctl = 75

    ctl_ratio = min(1.25, max(0.4, current_ctl / target_ctl if target_ctl else 1.0))
    load_factor = 1 + (1 - ctl_ratio) * 0.06

    if current_tsb < -15:
        fatigue_factor = 1.03
    elif current_tsb < -5:
        fatigue_factor = 1.015
    elif current_tsb > 10:
        fatigue_factor = 0.99
    else:
        fatigue_factor = 1.0

    race_projection = raw_projection * load_factor * fatigue_factor
    race_adjusted = race_projection * goal_context["elevation_factor"]

    flat_gap = race_projection - goal_context["goal_seconds"]
    adjusted_gap = race_adjusted - goal_context["goal_seconds"]

    probability = 85 - max(0, int(flat_gap / 60) * 2)
    probability += int((ctl_ratio - 1.0) * 20)
    probability = max(10, min(probability, 95))

    return {
        "anchor_distance": round(anchor_distance, 1),
        "current_projection": format_hms(raw_projection),
        "current_adjusted": format_hms(raw_projection * goal_context["elevation_factor"]),
        "race_projection": format_hms(race_projection),
        "race_adjusted": format_hms(race_adjusted),
        "flat_gap": format_gap(flat_gap),
        "adjusted_gap": format_gap(adjusted_gap),
        "probability": probability,
        "current_ctl": round(current_ctl, 1),
        "target_ctl": target_ctl,
        "race_projection_seconds": int(race_projection),
    }


def milestone_progress(athlete_id):
    rows = fetch_all_metrics(athlete_id)
    max_dist = 0.0
    max_dist_date = None

    for row in rows:
        distance = float(row["distance_km"] or 0)
        if distance < 40 and distance >= max_dist:
            max_dist = distance
            max_dist_date = _parse_timestamp(row["timestamp"]).date().isoformat()

    targets = [16, 18, 20, 22, 24, 26, 28, 30, 32]
    next_target = 32
    for target in targets:
        if max_dist < target:
            next_target = target
            break

    progress = int((max_dist / next_target) * 100) if next_target else 0
    return {
        "current_long": round(max_dist, 1),
        "current_long_date": max_dist_date,
        "next_target": next_target,
        "progress": min(progress, 100),
    }


def get_recent_activity_summaries(athlete_id, limit=3):
    rows = fetch_recent_metrics(athlete_id, limit=limit)
    activities = []

    for row in rows:
        ts = _parse_timestamp(row["timestamp"])
        activities.append(
            {
                "date": ts.date().isoformat(),
                "distance_km": round(float(row["distance_km"] or 0), 1),
                "duration": format_hms(float(row["moving_time_sec"] or 0)),
                "avg_hr": int(float(row["avg_hr"])) if row["avg_hr"] else None,
            }
        )

    return activities


def live_training_state(athlete_id):
    rows = fetch_all_metrics(athlete_id)
    if not rows:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        return {
            "weekly_km": 0.0,
            "weekly_stress": 0.0,
            "readiness": 0,
            "activity_done_today": False,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
        }

    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)

    weekly_rows = []
    for row in rows:
        run_date = _parse_timestamp(row["timestamp"]).date()
        if week_start <= run_date <= week_end:
            weekly_rows.append(row)

    weekly_km = sum(float(row["distance_km"] or 0) for row in weekly_rows)
    weekly_stress = sum(float(row["stress"] or 0) for row in weekly_rows)

    latest = rows[-1]
    latest_date = _parse_timestamp(latest["timestamp"]).date()
    activity_done_today = latest_date == today

    return {
        "weekly_km": round(weekly_km, 1),
        "weekly_stress": round(weekly_stress, 1),
        "readiness": int(float(latest["readiness"] or 0)),
        "activity_done_today": activity_done_today,
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
    }


def goal_snapshot(goal_context, intel, milestone):
    if not goal_context:
        return {
            "headline": "Set your race goal to get started.",
            "progress": 0,
            "status": "No goal found.",
        }

    if not intel:
        return {
            "headline": f"You are training for {goal_context['event_name']}",
            "progress": milestone["progress"],
            "status": "Sync more runs to unlock prediction.",
        }

    target_seconds = goal_context["goal_seconds"]
    projected_seconds = intel["race_projection_seconds"]
    if projected_seconds <= target_seconds:
        progress = 92
        status = "You are on pace for your goal. Stay consistent."
    else:
        delta_ratio = min(1.0, (projected_seconds - target_seconds) / target_seconds)
        progress = max(35, int(90 - (delta_ratio * 60)))
        minutes_off = int((projected_seconds - target_seconds) / 60)
        status = f"You are about {minutes_off} min away from goal pace right now."

    return {
        "headline": f"Goal: {goal_context['event_name']} in {goal_context['goal_time']}",
        "progress": progress,
        "status": status,
    }


def today_focus(live_state, intel):
    if live_state["activity_done_today"]:
        return "You have already trained today. Focus on recovery: hydration, mobility, and sleep."

    if not intel:
        return "Do an easy 30-40 min run today to start building consistency."

    probability = intel["probability"]
    if probability < 50:
        return "Today: easy aerobic run, 8-10 km at comfortable pace."
    if probability < 75:
        return "Today: steady run with 20 min comfortably hard effort."
    return "Today: race-specific session with controlled pace blocks."


def next_few_weeks_plan(intel):
    if not intel:
        return [
            "Week 1: Run 4 days, mostly easy.",
            "Week 2: Add one slightly longer run.",
            "Week 3: Keep consistency, do not skip easy days.",
            "Week 4: Repeat with slightly more distance.",
        ]

    probability = intel["probability"]
    if probability < 50:
        return [
            "Week 1: Build routine with 4 runs and 1 long easy run.",
            "Week 2: Increase long run by 2 km.",
            "Week 3: Add one short tempo segment.",
            "Week 4: Repeat volume, avoid sudden jumps.",
        ]
    if probability < 75:
        return [
            "Week 1: Keep 1 long run and 1 tempo workout.",
            "Week 2: Add marathon-pace blocks in long run.",
            "Week 3: Maintain volume, keep easy days easy.",
            "Week 4: Slightly reduce load for freshness.",
        ]
    return [
        "Week 1: One race-specific long workout.",
        "Week 2: Maintain volume with quality over quantity.",
        "Week 3: Reduce fatigue, keep legs fresh.",
        "Week 4: Taper and sharpen for race day.",
    ]


def rule_based_recommendation(intel):
    if not intel:
        return "Sync activities to generate a recommendation"

    probability = intel["probability"]
    if probability < 40:
        return "Aerobic Base Run: 12-16 km easy"
    if probability < 60:
        return "Long Run: 18-24 km steady"
    if probability < 75:
        return "Tempo Session: 6-10 km at threshold"
    return "Race Specific Workout"


def tomorrow_activity(intel, activity_done_today):
    if not intel:
        return "Tomorrow: Easy aerobic run for 30-40 minutes to restart consistency."

    probability = intel["probability"]
    if activity_done_today:
        if probability >= 70:
            return "Tomorrow: Recovery run 6-8 km easy with light mobility."
        return "Tomorrow: Easy run 8-10 km at conversational pace."

    if probability < 50:
        return "Tomorrow: Aerobic run 10-12 km easy with 4-6 strides."
    if probability < 75:
        return "Tomorrow: Tempo session 3 x 10 min threshold with easy recoveries."
    return "Tomorrow: Race-specific workout with marathon-pace blocks."
