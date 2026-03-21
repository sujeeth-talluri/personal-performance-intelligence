from datetime import timedelta


PLANNED = "PLANNED"
TODAY = "TODAY"
DONE = "DONE"
PARTIAL = "PARTIAL"
MISSED = "MISSED"
SKIPPED = "SKIPPED"
OVERDONE = "OVERDONE"
DIFFERENT_ACTIVITY = "DIFFERENT_ACTIVITY"

RUN = "RUN"
STRENGTH = "STRENGTH"
WALK = "WALK"
CROSS_TRAIN = "CROSS_TRAIN"
REST = "REST"

RUN_TYPES = {"run", "virtualrun", "trail run", "trail_run", "treadmill", "track"}
STRENGTH_TYPES = {"strength", "weight_training", "strength_training", "crossfit", "yoga", "pilates", "workout", "core", "flexibility"}
WALK_TYPES = {"walk", "hike"}
CROSS_TRAIN_TYPES = {"ride", "virtualride", "cycling", "swim", "swimming", "rowing", "elliptical", "stairstepper", "hiit", "aerobics"}

SESSION_NAMES = {
    "easy": "Easy Run",
    "long": "Long Run",
    "tempo": "Tempo Run",
    "recovery": "Recovery Run",
    "active_recovery": "Active Recovery",
    "intervals": "Interval Session",
    "marathon_pace": "Marathon Pace Run",
    "strength": "Strength & Conditioning",
    "rest": "Rest Day",
}

DAY_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
WEEKLY_SNAPSHOT_VERSION = 2


def classify_activity_type(activity_type):
    normalized = (activity_type or "").lower()
    if normalized in RUN_TYPES:
        return RUN
    if normalized in STRENGTH_TYPES:
        return STRENGTH
    if normalized in WALK_TYPES:
        return WALK
    if normalized in CROSS_TRAIN_TYPES:
        return CROSS_TRAIN
    return None


def planned_workout_from_ai(day_name, day_date, day_info):
    session_type = day_info.get("type", "rest")
    planned_km = float(day_info.get("km", 0) or 0.0)
    if session_type == "strength":
        workout_type = STRENGTH
        session_name = SESSION_NAMES["strength"]
        planned_km = 0.0
    elif session_type == "rest":
        workout_type = REST
        session_name = SESSION_NAMES["rest"]
        planned_km = 0.0
    else:
        workout_type = RUN
        session_name = SESSION_NAMES.get(session_type, session_type.replace("_", " ").title())
    return {
        "date": day_date.isoformat(),
        "day_name": day_name,
        "session_type": session_type,
        "workout_type": workout_type,
        "session_name": session_name,
        "planned_distance_km": round(planned_km, 1),
        "pace_guidance": day_info.get("pace_guidance", ""),
        "notes": day_info.get("notes", ""),
    }


def build_weekly_plan_snapshot(week_start, daily_plan):
    snapshot_days = {}
    for offset, day_name in enumerate(DAY_NAMES):
        day_date = week_start + timedelta(days=offset)
        snapshot_days[day_date.isoformat()] = planned_workout_from_ai(
            day_name,
            day_date,
            daily_plan.get(day_name, {"type": "rest", "km": 0, "notes": ""}),
        )
    weekly_target_km = round(
        sum(day["planned_distance_km"] for day in snapshot_days.values() if day["workout_type"] == RUN),
        1,
    )
    return {
        "version": WEEKLY_SNAPSHOT_VERSION,
        "week_start": week_start.isoformat(),
        "weekly_target_km": weekly_target_km,
        "days": snapshot_days,
    }


def weekly_snapshot_is_valid(snapshot):
    if not isinstance(snapshot, dict):
        return False
    if int(snapshot.get("version") or 0) != WEEKLY_SNAPSHOT_VERSION:
        return False
    days = snapshot.get("days") or {}
    if len(days) != 7:
        return False
    try:
        run_total = round(
            sum(
                float(day.get("planned_distance_km") or 0.0)
                for day in days.values()
                if day.get("workout_type") == RUN
            ),
            1,
        )
        frozen_target = round(float(snapshot.get("weekly_target_km") or 0.0), 1)
    except Exception:
        return False
    return abs(run_total - frozen_target) <= 0.1


def aggregate_actual_activities(activities, local_date_fn):
    by_date = {}
    for activity in activities:
        activity_date = local_date_fn(activity.date)
        bucket = by_date.setdefault(
            activity_date.isoformat(),
            {
                "run_distance_km": 0.0,
                "strength_count": 0,
                "walk_distance_km": 0.0,
                "cross_train_distance_km": 0.0,
                "has_any_activity": False,
            },
        )
        bucket["has_any_activity"] = True
        strict_type = classify_activity_type(getattr(activity, "activity_type", None))
        distance_km = float(getattr(activity, "distance_km", 0.0) or 0.0)
        if strict_type == RUN:
            bucket["run_distance_km"] = round(bucket["run_distance_km"] + distance_km, 1)
        elif strict_type == STRENGTH:
            bucket["strength_count"] += 1
        elif strict_type == WALK:
            bucket["walk_distance_km"] = round(bucket["walk_distance_km"] + distance_km, 1)
        elif strict_type == CROSS_TRAIN:
            bucket["cross_train_distance_km"] = round(bucket["cross_train_distance_km"] + distance_km, 1)
    return by_date


def derive_plan_state(planned_workout, actual_bucket, today_date):
    actual_bucket = actual_bucket or {}
    planned_date = planned_workout["date"]
    workout_type = planned_workout["workout_type"]
    planned_km = float(planned_workout.get("planned_distance_km") or 0.0)
    run_km = float(actual_bucket.get("run_distance_km") or 0.0)
    has_strength = int(actual_bucket.get("strength_count") or 0) > 0
    has_other_activity = bool(
        actual_bucket.get("walk_distance_km")
        or actual_bucket.get("cross_train_distance_km")
        or has_strength
    )
    if planned_date > today_date.isoformat():
        state = PLANNED
    elif workout_type == RUN:
        if run_km > max(1.5, planned_km * 1.2):
            state = OVERDONE
        elif run_km >= max(0.001, planned_km * 0.8):
            state = DONE
        elif run_km > 0:
            state = PARTIAL
        elif has_other_activity:
            state = DIFFERENT_ACTIVITY
        elif planned_date == today_date.isoformat():
            state = TODAY
        else:
            state = MISSED
    elif workout_type == STRENGTH:
        if has_strength:
            state = DONE
        elif has_other_activity or run_km > 0:
            state = DIFFERENT_ACTIVITY
        elif planned_date == today_date.isoformat():
            state = TODAY
        else:
            state = MISSED
    else:
        if actual_bucket.get("has_any_activity"):
            state = DIFFERENT_ACTIVITY
        elif planned_date == today_date.isoformat():
            state = TODAY
        else:
            state = PLANNED
    return {
        **planned_workout,
        "state": state,
        "actual_run_km": round(run_km, 1),
        "actual_strength_count": int(actual_bucket.get("strength_count") or 0),
        "actual_walk_km": round(float(actual_bucket.get("walk_distance_km") or 0.0), 1),
        "actual_cross_train_km": round(float(actual_bucket.get("cross_train_distance_km") or 0.0), 1),
    }

def build_week_plan_state(snapshot, actual_by_date, today_date):
    items = []
    for day_key in sorted(snapshot.get("days", {}).keys()):
        items.append(
            derive_plan_state(snapshot["days"][day_key], actual_by_date.get(day_key), today_date)
        )
    return items


def compute_week_metrics(plan_items, actual_by_date=None):
    planned_km = round(sum(item["planned_distance_km"] for item in plan_items if item["workout_type"] == RUN), 1)
    if actual_by_date is not None:
        actual_km = round(sum(float(bucket.get("run_distance_km") or 0.0) for bucket in actual_by_date.values()), 1)
    else:
        actual_km = round(sum(item["actual_run_km"] for item in plan_items), 1)
    remaining_km = round(max(0.0, planned_km - actual_km), 1)
    return {
        "planned_km": planned_km,
        "actual_km": actual_km,
        "remaining_km": remaining_km,
        "completed_runs": len([item for item in plan_items if item["workout_type"] == RUN and item["state"] in {DONE, OVERDONE}]),
        "longest_run_km": round(max([item["actual_run_km"] for item in plan_items if item["workout_type"] == RUN] or [0.0]), 1),
        "planned_long_run_km": round(max([item["planned_distance_km"] for item in plan_items if item.get("session_type") == "long"] or [0.0]), 1),
        "long_run_goal_met": any(
            item["workout_type"] == RUN
            and item["actual_run_km"] >= max(12.0, max([p["planned_distance_km"] for p in plan_items if p.get("session_type") == "long"] or [0.0]) * 0.8)
            for item in plan_items
        ),
        "quality_goal_met": any(
            item["workout_type"] == RUN
            and item.get("session_type") in {"tempo", "intervals", "marathon_pace"}
            and item["state"] in {DONE, OVERDONE}
            for item in plan_items
        ),
        "strength_goal_met": any(
            item["workout_type"] == STRENGTH and item["state"] == DONE
            for item in plan_items
        ),
    }


def today_session_from_plan(plan_items, today_date):
    for item in plan_items:
        if item["date"] == today_date.isoformat():
            return item
    return None
