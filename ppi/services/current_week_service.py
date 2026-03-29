"""Current-week dashboard helper functions.

This module contains all pure helper functions used by the dashboard pipeline
that compute the current week's plan, coaching messages, display metrics,
session verdicts, snapshot management, and workout-log persistence.

These were extracted from routes.py to keep the route file lean and to allow
the helpers to be tested independently of the Flask request context.
"""

import json
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import current_app

from ..repositories import (
    commit_all,
    delete_workout_log,
    fetch_activities_between,
    fetch_workout_logs,
    upsert_workout_log,
)
from ..services.plan_engine import (
    apply_adaptive_plan as service_apply_adaptive_plan,
    build_weekly_plan_template,
    classify_quality_completion as service_classify_quality_completion,
    classify_run_completion as service_classify_run_completion,
    goal_marathon_pace as service_goal_marathon_pace,
    plan_meta_for_session as service_plan_meta_for_session,
    select_best_run_for_session as service_select_best_run_for_session,
    training_consistency_score as service_training_consistency_score,
)
from ..services.training_state_engine import (
    build_weekly_plan_snapshot,
    weekly_snapshot_is_valid,
)
from ..models import CoachingPlan
from ..extensions import db


def _today_date_label(user_timezone):
    try:
        tz = ZoneInfo(user_timezone)
    except ZoneInfoNotFoundError:
        fallback = {
            "asia/kolkata": timezone(timedelta(hours=5, minutes=30)),
            "asia/calcutta": timezone(timedelta(hours=5, minutes=30)),
            "utc": timezone.utc,
            "etc/utc": timezone.utc,
        }
        tz = fallback.get((user_timezone or "").lower(), timezone.utc)
    return datetime.now(timezone.utc).astimezone(tz).strftime("%b %d, %Y")


def _get_coaching_plan_row(user_id):
    return CoachingPlan.query.filter_by(user_id=user_id).order_by(CoachingPlan.id.desc()).first()


def _load_coaching_freeze_state(plan_row):
    if not plan_row or not plan_row.context_json:
        return {}
    try:
        context = json.loads(plan_row.context_json)
    except Exception:
        return {}
    freeze_state = context.get("freeze_state")
    return freeze_state if isinstance(freeze_state, dict) else {}


def _save_coaching_freeze_state(plan_row, freeze_state):
    if not plan_row:
        return
    try:
        context = json.loads(plan_row.context_json) if plan_row.context_json else {}
    except Exception:
        context = {}
    context["freeze_state"] = freeze_state
    plan_row.context_json = json.dumps(context, default=str)
    db.session.add(plan_row)
    db.session.commit()


def _load_or_create_weekly_snapshot(plan_row, week_start, daily_plan):
    freeze_state = _load_coaching_freeze_state(plan_row)
    snapshots = freeze_state.setdefault("weekly_plan_snapshots", {})
    key = week_start.isoformat()
    snapshot = snapshots.get(key)
    if snapshot and not weekly_snapshot_is_valid(snapshot):
        snapshot = None
    if not snapshot:
        snapshot = build_weekly_plan_snapshot(week_start, daily_plan)
        snapshot["schedule_locked"] = True
        snapshots[key] = snapshot
        _save_coaching_freeze_state(plan_row, freeze_state)
    return snapshot


def _session_type_from_template_session(session_name):
    mapping = {
        "Race Day": "race",
        "Long Run": "long",
        "Medium Long Run": "steady",
        "Tempo Run": "tempo",
        "Speed Session": "intervals",
        "Marathon Pace Run": "marathon_pace",
        "Aerobic Run": "aerobic",
        "Steady Run": "steady",
        "Easy Run": "easy",
        "Recovery Run": "recovery",
        "Strength": "strength",
        "Rest": "rest",
    }
    return mapping.get(session_name, "easy")


def _snapshot_needs_schedule_repair(snapshot, profile, week_start):
    if not snapshot or snapshot.get("schedule_locked"):
        return False
    if not profile or not getattr(profile, "updated_at", None):
        return False
    try:
        updated_local = profile.updated_at.date()
    except Exception:
        return False
    return updated_local >= week_start


def _replace_weekly_snapshot(plan_row, week_start, daily_plan):
    freeze_state = _load_coaching_freeze_state(plan_row)
    snapshots = freeze_state.setdefault("weekly_plan_snapshots", {})
    snapshot = build_weekly_plan_snapshot(week_start, daily_plan)
    snapshot["schedule_locked"] = True
    snapshots[week_start.isoformat()] = snapshot
    _save_coaching_freeze_state(plan_row, freeze_state)
    return snapshot


def _repair_snapshot_zero_long_run(plan_row, week_start, snapshot, daily_plan):
    """Repair a frozen snapshot whose Long Run day has planned_distance_km == 0.

    A snapshot can be frozen with Long Run km = 0 if it was first created
    before the plan engine had sufficient data (e.g. weekly_goal_km = 0 on
    an early page load).  When that happens `weekly_snapshot_is_valid` still
    passes because it only checks whether run_total ≈ weekly_target_km — both
    of which are 0 for the Long Run.

    If the fresh daily_plan now produces a nonzero Long Run the snapshot is
    rebuilt from the fresh plan so the correct mileage is displayed.

    Returns (snapshot, was_repaired).
    """
    if not snapshot or not plan_row:
        return snapshot, False
    days = snapshot.get("days") or {}
    has_zero_long_run = any(
        float(day.get("planned_distance_km") or 0.0) == 0.0
        and day.get("session_name") == "Long Run"
        for day in days.values()
    )
    if not has_zero_long_run:
        return snapshot, False
    # Only repair if the fresh plan actually has a meaningful Long Run km.
    fresh_long_run_km = max(
        (float(v.get("km") or 0.0) for v in daily_plan.values() if v.get("type") == "long"),
        default=0.0,
    )
    if fresh_long_run_km <= 0:
        return snapshot, False
    repaired = _replace_weekly_snapshot(plan_row, week_start, daily_plan)
    return repaired, True


def _secs_to_pace_str(secs):
    """Convert seconds per km to 'M:SS/km' string."""
    secs = max(0, int(round(secs)))
    return f"{secs // 60}:{secs % 60:02d}/km"


def _pace_guidance_for_session(session_name, goal_pace_sec_per_km=None):
    """Return pace guidance for a session.

    When goal_pace_sec_per_km is provided, returns actual pace ranges like
    '5:30–6:00/km'. Falls back to descriptive text if pace is unavailable.
    """
    descriptive = {
        "Long Run": "Easy conversational pace",
        "Medium Long Run": "Controlled easy-to-steady aerobic effort",
        "Recovery Run": "Very easy recovery effort",
        "Easy Run": "Relaxed conversational pace",
        "Aerobic Run": "Comfortable aerobic effort",
        "Steady Run": "Controlled steady effort",
        "Tempo Run": "Comfortably hard tempo effort",
        "Speed Session": "Fast, controlled quality work",
        "Marathon Pace Run": "Settle into goal marathon pace",
        "Race Day": "Race execution pace",
    }
    # Normalise short/legacy names that snapshots may have baked in
    _aliases = {
        "Aerobic":     "Aerobic Run",
        "Medium Long": "Medium Long Run",
        "Speed":       "Speed Session",
        "Intervals":   "Interval Session",
        "Easy":        "Easy Run",
        "Long":        "Long Run",
        "Recovery":    "Recovery Run",
        "Tempo":       "Tempo Run",
    }
    session_name = _aliases.get(session_name, session_name)
    if not goal_pace_sec_per_km or goal_pace_sec_per_km <= 0:
        return descriptive.get(session_name, "")
    gp = goal_pace_sec_per_km
    # Offset ranges (seconds/km) relative to goal marathon pace.
    # Negative = faster; Positive = slower.
    zones = {
        "Recovery Run":     (gp + 80,  gp + 100),
        "Easy Run":         (gp + 55,  gp + 75),
        "Aerobic Run":      (gp + 40,  gp + 65),
        "Long Run":         (gp + 40,  gp + 70),
        "Medium Long Run":  (gp + 25,  gp + 55),
        "Steady Run":       (gp + 10,  gp + 30),
        "Tempo Run":        (gp - 15,  gp + 5),
        "Speed Session":    (gp - 30,  gp - 10),
        "Marathon Pace Run":(gp -  5,  gp + 5),
        "Race Day":         (gp -  5,  gp + 5),
    }
    if session_name in zones:
        lo, hi = zones[session_name]
        return f"{_secs_to_pace_str(lo)}–{_secs_to_pace_str(hi)}"
    return descriptive.get(session_name, "")


def _schedule_preferences_from_profile(profile):
    if not profile:
        return {
            "training_days_per_week": 5,
            "long_run_day": "sunday",
            "strength_days_per_week": 2,
        }
    return {
        "training_days_per_week": int(getattr(profile, "training_days_per_week", 5) or 5),
        "long_run_day": str(getattr(profile, "long_run_day", "sunday") or "sunday").lower(),
        "strength_days_per_week": int(getattr(profile, "strength_days_per_week", 2) or 2),
    }


def _deterministic_current_week_daily_plan(intel, week_start, schedule_prefs=None):
    weekly = intel.get("weekly") or {}
    goal = intel.get("goal") or {}
    long_run = intel.get("long_run") or {}
    schedule_prefs = dict(schedule_prefs or {})
    weekly_goal = {
        "weekly_goal_km": float(weekly.get("weekly_goal_km") or 0.0),
        "phase": weekly.get("phase") or weekly.get("display_phase") or "base",
        "rebuild_mode": bool(weekly.get("rebuild_mode")),
        "ctl_proxy": float(weekly.get("ctl_proxy") or intel.get("current_ctl") or 0.0),
        "prior_avg_km": float(weekly.get("prior_avg_km") or 0.0),
        "training_consistency_ratio": float(weekly.get("training_consistency_ratio") or 0.0),
        "weeks_to_race": float(weekly.get("weeks_to_race") or max(0.0, float(goal.get("days_remaining") or 0.0) / 7.0)),
        "race_distance_km": float(weekly.get("race_distance_km") or goal.get("distance_km") or 42.195),
        "race_date": weekly.get("race_date") or goal.get("race_date"),
        "week_start": week_start.isoformat(),
        "goal_marathon_pace_sec_per_km": float(weekly.get("goal_marathon_pace_sec_per_km") or 0.0),
        "high_fatigue": bool(weekly.get("high_fatigue")),
        "moderate_fatigue": bool(weekly.get("moderate_fatigue")),
        "atl_spike": bool(weekly.get("atl_spike")),
        "training_days_per_week": int(schedule_prefs.get("training_days_per_week") or 5),
        "long_run_day": str(schedule_prefs.get("long_run_day") or "sunday").lower(),
        "strength_days_per_week": int(schedule_prefs.get("strength_days_per_week") or 2),
    }
    template = _weekly_plan_template(weekly_goal, long_run)
    day_names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    daily_plan = {}
    for idx, day_name in enumerate(day_names):
        item = template[idx]
        session_name = item["session"]
        daily_plan[day_name] = {
            "type": _session_type_from_template_session(session_name),
            "km": round(float(item.get("target_km") or 0.0), 1),
            "pace_guidance": _pace_guidance_for_session(
                session_name,
                weekly_goal.get("goal_marathon_pace_sec_per_km"),
            ),
            "notes": item.get("purpose", ""),
        }
    return daily_plan


def _infer_session_type_from_log(session_name, workout_type, fallback_type):
    if workout_type == "STRENGTH":
        return "strength"
    if workout_type == "REST":
        return "rest"
    normalized = (session_name or "").strip().lower()
    mapping = {
        "easy run": "easy",
        "long run": "long",
        "tempo run": "tempo",
        "recovery run": "recovery",
        "active recovery": "active_recovery",
        "interval session": "intervals",
        "marathon pace run": "marathon_pace",
        "medium long run": "steady",
        "steady run": "steady",
        "aerobic run": "aerobic",
        "race day": "race",
    }
    return mapping.get(normalized, fallback_type)


def _build_current_week_coaching_message(
    weekly_target_km,
    actual_km,
    longest_run_km,
    planned_long_run_km,
    long_run_goal_met,
    quality_goal_met,
    today_item,
    quality_session_name=None,
    recent_long_run_km=0.0,
    recent_long_run_date_text="",
):
    target_text = f"{weekly_target_km:.0f} km" if weekly_target_km else "your planned volume"
    progress_text = f"You have completed {actual_km:.1f} km so far out of {target_text} this week."
    if planned_long_run_km > 0:
        if long_run_goal_met:
            long_run_text = (
                f"Your long-run target for the week is already covered with a longest run of "
                f"{longest_run_km:.1f} km."
            )
        else:
            long_run_text = (
                f"The key remaining endurance job is a run of about {planned_long_run_km:.0f} km this week. "
                f"This week's longest run so far is {longest_run_km:.1f} km."
            )
    else:
        long_run_text = f"This week's longest run so far is {longest_run_km:.1f} km."

    if recent_long_run_km > longest_run_km and recent_long_run_date_text:
        long_run_text += (
            f" Your most recent training long run was {recent_long_run_km:.1f} km on "
            f"{recent_long_run_date_text}."
        )

    if quality_session_name:
        quality_text = (
            f"Your {quality_session_name.lower()} is already done."
            if quality_goal_met
            else f"Your {quality_session_name.lower()} is still open, so protect that session if fatigue stays under control."
        )
    else:
        quality_text = "No quality session is scheduled this week."

    today_text = ""
    if today_item:
        if today_item["status"] == "different_activity":
            if today_item.get("workout_type") == "RUN":
                today_text = " You did other activity today, but the planned run still remains open."
            elif today_item.get("workout_type") == "STRENGTH":
                today_text = " You did a run today, but the planned strength session still remains open."
            else:
                today_text = " You did other activity today, but the planned session still remains open."
        elif today_item["status"] == "partial":
            today_text = " Today's planned session was only partially completed, so keep the next run controlled."
        elif today_item["status"] == "overdone":
            today_text = " Today ended heavier than planned, so the next run should stay conservative."

    return f"{progress_text} {long_run_text} {quality_text}{today_text}".strip()


def _derive_current_week_display_metrics(weekly_plan, frozen_weekly_target_km):
    actual_km = round(
        sum(
            float(item.get("actual_km") or 0.0)
            for item in weekly_plan
        ),
        1,
    )
    longest_run_km = round(
        max(
            [
                float(item.get("actual_km") or 0.0)
                for item in weekly_plan
            ]
            or [0.0]
        ),
        1,
    )
    planned_long_run_km = round(
        max(
            [
                float(item.get("planned_km") or 0.0)
                for item in weekly_plan
                if item.get("workout_type") == "RUN" and item.get("session") == "Long Run"
            ]
            or [0.0]
        ),
        1,
    )
    long_run_goal_met = (
        longest_run_km >= max(12.0, round(planned_long_run_km * 0.8, 1))
        if planned_long_run_km > 0
        else False
    )
    quality_goal_met = any(
        item.get("workout_type") == "RUN"
        and item.get("session") in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"}
        and item.get("status") in {"completed", "overdone"}
        for item in weekly_plan
    )
    quality_session = next(
        (
            item for item in weekly_plan
            if item.get("workout_type") == "RUN"
            and item.get("session") in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"}
        ),
        None,
    )
    strength_goal_met = any(
        item.get("workout_type") == "STRENGTH"
        and item.get("status") == "completed"
        for item in weekly_plan
    )
    remaining_km = round(max(0.0, float(frozen_weekly_target_km or 0.0) - actual_km), 1)
    progress_pct = min(100, round(actual_km / max(1.0, float(frozen_weekly_target_km or 0.0)) * 100, 1))
    return {
        "weekly_target_km": round(float(frozen_weekly_target_km or 0.0), 1),
        "actual_km": actual_km,
        "remaining_km": remaining_km,
        "longest_run_km": longest_run_km,
        "planned_long_run_km": planned_long_run_km,
        "long_run_goal_met": long_run_goal_met,
        "quality_goal_met": quality_goal_met,
        "quality_session_name": quality_session.get("session") if quality_session else None,
        "quality_session_day": quality_session.get("day") if quality_session else None,
        "strength_goal_met": strength_goal_met,
        "progress_pct": progress_pct,
    }


def _format_alternate_activity_text(item, is_today=False):
    walk_km = round(float(item.get("actual_walk_km") or 0.0), 1)
    cross_km = round(float(item.get("actual_cross_train_km") or 0.0), 1)
    run_km = round(float(item.get("actual_km") or 0.0), 1)
    strength_count = int(item.get("actual_strength_count") or 0)
    workout_type = item.get("workout_type")
    session_type = item.get("session_type")

    if workout_type == "RUN":
        if is_today:
            if walk_km > 0:
                return f"{walk_km:.1f}km walk done - run still open"
            if cross_km > 0:
                return "Cross-training done - run still open"
            if strength_count > 0:
                return "Gym done - run still open"
            return "Other activity done - run still open"
        if walk_km > 0:
            return f"Run missed - {walk_km:.1f}km walk done"
        if cross_km > 0:
            return "Run missed - cross-training done"
        if strength_count > 0:
            return "Run missed - gym done"
        return "Run missed - other activity done"

    if workout_type == "STRENGTH":
        if is_today:
            if run_km > 0:
                return f"{run_km:.1f}km run done - gym still open"
            if walk_km > 0:
                return f"{walk_km:.1f}km walk done - gym still open"
            if cross_km > 0:
                return "Cross-training done - gym still open"
            return "Other activity done - gym still open"
        if run_km > 0:
            return f"{run_km:.1f}km run done instead of gym"
        if walk_km > 0:
            return f"Gym missed - {walk_km:.1f}km walk done"
        if cross_km > 0:
            return "Gym missed - cross-training done"
        return "Gym missed - other activity done"

    if session_type == "active_recovery" and walk_km > 0:
        return f"Walk done - {walk_km:.1f}km"
    if cross_km > 0:
        return "Cross-training done"
    if walk_km > 0:
        return f"{walk_km:.1f}km walk done"
    if run_km > 0:
        return f"{run_km:.1f}km run done"
    if strength_count > 0:
        return "Gym done"
    return "Other activity done"


def _build_session_verdict(today_item):
    """Return a short verdict string for a completed session, or None."""
    if not today_item or not today_item.get("done"):
        return None
    actual  = float(today_item.get("actual_km") or 0.0)
    planned = float(today_item.get("planned_km") or 0.0)
    wtype   = today_item.get("workout_type", "")
    if wtype == "STRENGTH":
        return "Gym session completed ✓"
    if wtype != "RUN" or planned <= 0:
        return None
    ratio = actual / planned
    if ratio >= 1.08:
        return f"⚡ {actual:.1f} km — {actual - planned:.1f} km over target"
    if ratio >= 0.95:
        return f"✓ {actual:.1f} km — target nailed"
    if ratio >= 0.70:
        return f"Partial — {actual:.1f} of {planned:.1f} km done"
    return None


def _different_activity_status_label(item, is_today=False):
    if item.get("workout_type") == "RUN":
        return "Run open" if is_today else "Run missed"
    if item.get("workout_type") == "STRENGTH":
        return "Gym open" if is_today else "Gym missed"
    return "Other activity"


def _deterministic_phase_label(intel):
    weekly = intel.get("weekly") or {}
    base_phase = str(weekly.get("display_phase") or weekly.get("base_phase") or weekly.get("phase") or "training").title()
    week_type = str(weekly.get("week_type") or "").strip()
    suppressed_week_types = {"cutback week", "cutback", "recovery week"}
    if week_type.lower() in suppressed_week_types:
        return base_phase
    if week_type and week_type.lower() not in {base_phase.lower(), "race week"}:
        return f"{base_phase} · {week_type}"
    return base_phase


def _deterministic_feasibility_fields(
    intel,
    current_week_model,
    display_weekly_target_km=None,
    display_long_run_target_km=None,
):
    score = int(round(float(intel.get("marathon_readiness_pct") or 0.0)))
    score = max(0, min(100, score))
    status = str(intel.get("marathon_readiness_status") or "").strip().lower()
    next_step = str(intel.get("marathon_readiness_next_step") or "").strip()
    weekly = intel.get("weekly") or {}
    goal = intel.get("goal") or {}
    actual_km = float(current_week_model.get("actual_km") or 0.0)
    weekly_target = float(
        display_weekly_target_km
        if display_weekly_target_km is not None
        else (current_week_model.get("weekly_target_km") or 0.0)
    )
    long_run_target = float(
        display_long_run_target_km
        if display_long_run_target_km is not None
        else (current_week_model.get("planned_long_run_km") or 0.0)
    )
    days_remaining = int(goal.get("days_remaining") or 0)

    # Use goal_alignment (probability-based) as the primary label when a valid
    # prediction exists — it's more honest than the readiness score alone.
    # A score of 70/100 while 42 minutes off goal should NOT read "On Track".
    goal_alignment = str(intel.get("goal_alignment") or "").strip()
    probability    = intel.get("probability")  # 0–100 or None

    _ALIGNMENT_COLOR = {
        "On Track":    "green",
        "Within Reach":"amber",
        "Building":    "amber",
        "Stretch":     "grey",
    }
    if goal_alignment and goal_alignment not in ("Too early to compare", "--"):
        label = goal_alignment
        color = _ALIGNMENT_COLOR.get(goal_alignment, "grey")
    elif score >= 70:
        color = "amber"
        label = "Building"
    elif score >= 55:
        color = "amber"
        label = "Building"
    else:
        color = "grey"
        label = "Needs Support"

    if actual_km > 0 and weekly_target > 0:
        _weeks_left  = max(1, days_remaining // 7)
        _week_done   = actual_km >= weekly_target * 0.85
        _lr_done     = long_run_target > 0 and actual_km >= long_run_target
        if _week_done:
            summary = (
                f"You've completed {actual_km:.1f} km of {weekly_target:.0f} km this week — week target met! "
                f"Stay consistent over the next {_weeks_left} weeks to build toward your goal."
            )
        elif _lr_done:
            summary = (
                f"You've completed {actual_km:.1f} km of {weekly_target:.0f} km this week. "
                f"Long run done — keep easy miles to round out the week and stay consistent over the next {_weeks_left} weeks."
            )
        elif long_run_target > 0:
            summary = (
                f"You've completed {actual_km:.1f} km of {weekly_target:.0f} km this week. "
                f"Key remaining: {long_run_target:.0f} km long run. Stay consistent over the next {_weeks_left} weeks."
            )
        else:
            summary = (
                f"You've completed {actual_km:.1f} km of {weekly_target:.0f} km this week. "
                f"Stay consistent over the next {_weeks_left} weeks."
            )
    else:
        summary = next_step or "Readiness will improve as weekly mileage, long runs, and consistency build together."

    return {
        "score": score,
        "color": color,
        "label": label,
        "text": summary,
    }


def _persist_snapshot_workout_logs(user_id, snapshot):
    """Write snapshot planned distances to WorkoutLog rows.

    For each day in the snapshot:
    - If no row exists → insert a new 'planned' row.
    - If an engine-sourced 'planned' row already exists with the WRONG
      target_distance_km (e.g. None when the snapshot was repaired from
      0 → correct km) → update target_distance_km so /api/dashboard picks
      up the canonical km instead of the stale analytics-derived value.

    Rows that are already completed/partial are never overwritten.
    """
    existing_logs = {
        log.workout_date: log
        for log in fetch_workout_logs(
            user_id,
            date.fromisoformat(min(snapshot.get("days", {}).keys())),
            date.fromisoformat(max(snapshot.get("days", {}).keys())),
        )
    } if snapshot.get("days") else {}

    for day_key, planned in snapshot.get("days", {}).items():
        day_date = date.fromisoformat(day_key)
        planned_km = float(planned.get("planned_distance_km") or 0.0)
        target_km = planned_km if planned_km > 0 else None

        existing = existing_logs.get(day_date)
        if existing:
            # Update engine-sourced planned rows whose target has drifted
            # (covers the snapshot-repair case where LR was 0 → now correct).
            if (
                existing.source == "engine"
                and existing.status == "planned"
                and float(existing.target_distance_km or 0.0) != (target_km or 0.0)
            ):
                upsert_workout_log(
                    user_id=user_id,
                    workout_date=day_date,
                    workout_type=planned["workout_type"],
                    session_name=planned["session_name"],
                    target_distance_km=target_km,
                    status="planned",
                    actual_distance_km=None,
                    notes=planned.get("notes", ""),
                    source="engine",
                    auto_commit=False,
                )
        else:
            upsert_workout_log(
                user_id=user_id,
                workout_date=day_date,
                workout_type=planned["workout_type"],
                session_name=planned["session_name"],
                target_distance_km=target_km,
                status="planned",
                actual_distance_km=None,
                notes=planned.get("notes", ""),
                source="engine",
                auto_commit=False,
            )
    commit_all()


def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _should_sync_now(last_sync_at, cooldown_min):
    if not last_sync_at:
        return True
    return (datetime.now(timezone.utc) - last_sync_at) >= timedelta(minutes=cooldown_min)


def fix_coaching_numbers(msg, weekly_target, long_run):
    """Replace stale km numbers in coaching messages with canonical values.

    Handles the full range of patterns the AI coach produces:
      - Weekly ranges/totals: "25-28km", "42.0 km this week", "targeting 42km"
      - Long run: "long run of 14km", "14 km on the long run",
                  "Long Run — 14 km", "14km long run"
    """
    # ── Weekly target ────────────────────────────────────────────────────────
    # Ranges: "25-28km" or "25 to 28km"
    msg = re.sub(
        r'\b\d+\.?\d*\s*(?:to|-)\s*\d+\.?\d*\s*km'
        r'(?:\s*(?:per week|weekly|target|a week))?',
        f'{weekly_target:.0f} km',
        msg, flags=re.IGNORECASE
    )
    # "42.0 km this week" / "42km per week" / "42km weekly" / "42km target"
    msg = re.sub(
        r'\b\d+\.?\d*\s*km\s*(?:per week|weekly|target|a week|this week)',
        f'{weekly_target:.0f} km this week',
        msg, flags=re.IGNORECASE
    )
    # "targeting 42km" / "Target 42.0 km" / "target of 42km"
    # Use a replacement function to preserve the original capitalisation of
    # the trigger word ("Target" stays "Target", "targeting" stays "targeting").
    def _repl_target(m):
        word = m.group(0).split()[0]            # "Target" / "targeting" / "target"
        return f'{word} {weekly_target:.0f} km'
    msg = re.sub(
        r'(?:target(?:ing)?(?:\s+of)?)\s+\d+\.?\d*\s*km',
        _repl_target,
        msg, flags=re.IGNORECASE
    )

    # ── Long run (only when we have a valid canonical value) ─────────────────
    if long_run and long_run > 0:
        lr = f'{long_run:.0f}'
        # "long run of 14km" / "long-run of 14 km" (mandatory "of")
        msg = re.sub(
            r'(?:long run|long-run)\s+of\s+(?:\d+\.?\d*\s*(?:to|-)\s*)?\d+\.?\d*\s*km',
            f'long run of {lr} km',
            msg, flags=re.IGNORECASE
        )
        # "14 km on the long run" / "14km on the long run"
        msg = re.sub(
            r'\b\d+\.?\d*\s*km\s+on\s+the\s+(?:long run|long-run)',
            f'{lr} km on the long run',
            msg, flags=re.IGNORECASE
        )
        # "Long Run — 14 km" / "Long Run - 14km" / "Long Run – 14 km"
        msg = re.sub(
            r'(?:long run|long-run)\s*[—–\-]+\s*\d+\.?\d*\s*km',
            f'Long Run — {lr} km',
            msg, flags=re.IGNORECASE
        )
        # "14km long run" (number precedes the label)
        msg = re.sub(
            r'\b\d+\.?\d*\s*km\s+(?:long run|long-run)',
            f'{lr} km long run',
            msg, flags=re.IGNORECASE
        )
        # "with 14 km on the long" (partial phrase variant)
        msg = re.sub(
            r'(?<=with\s)\d+\.?\d*\s*km\s+on\s+the\s+long',
            f'{lr} km on the long',
            msg, flags=re.IGNORECASE
        )
    return msg


def _today_local_date(user_timezone):
    try:
        tz = ZoneInfo(user_timezone)
    except ZoneInfoNotFoundError:
        tz = timezone.utc
    now_local = datetime.now(timezone.utc).astimezone(tz)
    return now_local.date()


def _now_local_datetime(user_timezone):
    try:
        tz = ZoneInfo(user_timezone)
    except ZoneInfoNotFoundError:
        tz = timezone.utc
    return datetime.now(timezone.utc).astimezone(tz)


def _week_bounds(today_local):
    start = today_local - timedelta(days=today_local.weekday())
    end = start + timedelta(days=6)
    return start, end


def _activity_local_date(dt_value, user_timezone):
    try:
        tz = ZoneInfo(user_timezone)
    except ZoneInfoNotFoundError:
        fallback = {
            "asia/kolkata": timezone(timedelta(hours=5, minutes=30)),
            "asia/calcutta": timezone(timedelta(hours=5, minutes=30)),
            "utc": timezone.utc,
            "etc/utc": timezone.utc,
        }
        tz = fallback.get((user_timezone or "").lower(), timezone.utc)
    if dt_value.tzinfo is None:
        dt_utc = dt_value.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_value.astimezone(timezone.utc)
    return dt_utc.astimezone(tz).date()


def _weekly_plan_template(weekly_goal, long_run):
    return build_weekly_plan_template(weekly_goal, long_run)


def _classify_run_completion(actual_km, target_km):
    return service_classify_run_completion(actual_km, target_km)


def _status_label(status):
    mapping = {
        "completed": "Completed",
        "moved": "Moved",
        "partial": "Partial",
        "missed": "Missed",
        "skipped": "Missed",
        "planned": "Planned",
        "overperformed": "Completed",
    }
    return mapping.get(status, status.title())


def _goal_marathon_pace(weekly_goal):
    return service_goal_marathon_pace(weekly_goal)


def _run_pace_sec_per_km(activity):
    distance = float(activity.distance_km or 0.0)
    moving = float(activity.moving_time or 0.0)
    if distance <= 0 or moving <= 0:
        return None
    return moving / distance


def _select_best_run_for_session(run_acts, session_name, weekly_goal):
    return service_select_best_run_for_session(run_acts, session_name, weekly_goal, _run_pace_sec_per_km)


def _classify_quality_completion(session_name, actual_km, target_km, pace_sec_per_km, weekly_goal):
    return service_classify_quality_completion(session_name, actual_km, target_km, pace_sec_per_km, weekly_goal)


def _priority_rank(item):
    order = {
        "Race Day": 0,
        "Long Run": 1,
        "Marathon Pace Run": 2,
        "Speed Session": 3,
        "Tempo Run": 4,
        "Aerobic Run": 5,
        "Medium Long Run": 6,
        "Steady Run": 7,
        "Easy Run": 8,
        "Recovery Run": 9,
        "Strength": 10,
    }
    return order.get(item.get("session"), 99)


def _plan_meta_for_session(session_name):
    return service_plan_meta_for_session(session_name)


def _fatigue_score(plan_items, weekly_goal_km, today_local):
    score = 0
    recent_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"] and item["date"] < today_local]
    last_two_days_km = sum(item["actual_km"] or 0.0 for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"] and 0 <= (today_local - item["date"]).days <= 2)
    missed_key_sessions = [
        item for item in plan_items
        if item["date"] < today_local and item["workout_type"] == "RUN" and item["session"] in {"Speed Session", "Marathon Pace Run", "Tempo Run", "Long Run"} and item["status"] in {"missed", "partial"}
    ]
    if weekly_goal_km > 0 and last_two_days_km >= weekly_goal_km * 0.35:
        score += 1
    if any(item["session"] == "Long Run" and item["status"] == "completed" and 0 <= (today_local - item["date"]).days <= 2 for item in recent_runs):
        score += 1
    if len(missed_key_sessions) >= 1:
        score += 1
    if sum(item["actual_km"] or 0.0 for item in recent_runs) >= weekly_goal_km * 0.8:
        score += 1
    return score


def _apply_adaptive_plan(plan_items, today_local, weekly_goal):
    return service_apply_adaptive_plan(plan_items, today_local, weekly_goal)


def _build_weekly_plan(user_id, today_local, user_timezone, weekly_goal, long_run, week_start=None, persist=True):
    week_start = week_start or _week_bounds(today_local)[0]
    week_end = week_start + timedelta(days=6)
    template = _weekly_plan_template(weekly_goal, long_run)
    # Canonical long run distance from the ladder — used to detect stale Sunday rows.
    canonical_long_km = round(float(long_run.get("next_milestone_km") or 0), 1)

    existing = {w.workout_date: w for w in fetch_workout_logs(user_id, week_start, week_end)}
    for offset in range(7):
        day_date = week_start + timedelta(days=offset)
        plan = template[offset]
        if day_date in existing:
            row = existing[day_date]
            # Overwrite engine-generated rows that are still planned —
            # this lets template changes take effect without wiping user edits
            # or completed/partial entries.
            if row.source == "engine" and row.status == "planned":
                # FREEZE past days — never overwrite planned distance after the day
                # has passed. The athlete may have missed it; changing the target
                # retroactively corrupts historical compliance data.
                if day_date < today_local:
                    continue
                # FIX 7: Sunday long run — if target_km doesn't match the canonical
                # ladder distance, delete the stale row and regenerate it below.
                if (offset == 6 and canonical_long_km > 0 and plan["workout_type"] == "RUN"
                        and abs(float(row.target_distance_km or 0) - plan["target_km"]) > 0.5):
                    delete_workout_log(user_id, day_date)
                    # Fall through to upsert with the correct target_km
                else:
                    row.workout_type = plan["workout_type"]
                    row.session_name = plan["session"]
                    row.target_distance_km = plan["target_km"]
                    continue
            else:
                continue
        upsert_workout_log(
            user_id=user_id,
            workout_date=day_date,
            workout_type=plan["workout_type"],
            session_name=plan["session"],
            target_distance_km=plan["target_km"],
            status="planned",
            source="engine",
            auto_commit=False,
        )
    commit_all()

    start_dt = datetime.combine(week_start - timedelta(days=1), datetime.min.time())
    end_dt = datetime.combine(week_end + timedelta(days=1), datetime.max.time())
    activities = fetch_activities_between(user_id, start_dt, end_dt)
    by_day = {}
    for activity in activities:
        local_day = _activity_local_date(activity.date, user_timezone)
        by_day.setdefault(local_day, []).append(activity)

    logs = fetch_workout_logs(user_id, week_start, week_end)
    used_activity_ids = set()
    notes_by_date = {}

    def _run_candidates_for_session(day_acts, session_name):
        if session_name == "Race Day":
            return [a for a in day_acts if (a.activity_type or "").lower() in {"run", "trailrun"} and bool(a.is_race)]
        return [a for a in day_acts if (a.activity_type or "").lower() in {"run", "trailrun"} and not a.is_race and getattr(a, "strava_activity_id", None) not in used_activity_ids]

    def _match_session_on_day(day_acts, session_name):
        candidates = _run_candidates_for_session(day_acts, session_name)
        if not candidates:
            return None, 0.0, None, []
        if session_name in {"Race Day", "Long Run", "Medium Long Run", "Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"}:
            matched = _select_best_run_for_session(candidates, session_name, weekly_goal)
            if not matched:
                return None, 0.0, None, []
            return matched, round(float(matched.distance_km), 1), _run_pace_sec_per_km(matched), [matched]
        return None, round(sum(a.distance_km for a in candidates), 1), None, candidates

    session_priority_logs = sorted(
        logs,
        key=lambda log: (
            0 if log.workout_type == "RUN" else 1,
            _priority_rank({"session": log.session_name}),
            log.workout_date,
        ),
    )

    computed = {}
    movable_sessions = {"Long Run", "Medium Long Run", "Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run", "Aerobic Run"}

    for log in session_priority_logs:
        acts = by_day.get(log.workout_date, [])
        strength_done = any((a.activity_type or "").lower() in {"strength", "yoga"} for a in acts)
        moved_note = None
        moved_status = False
        matched_items = []
        matched_run, run_km, run_pace, matched_items = _match_session_on_day(acts, log.session_name)

        if log.workout_type == "RUN" and run_km <= 0 and log.workout_date <= today_local and log.session_name in movable_sessions:
            best_day = None
            best_result = (None, 0.0, None, [])
            for candidate_day in sorted(by_day.keys()):
                if candidate_day == log.workout_date:
                    continue
                if not (week_start <= candidate_day <= min(today_local, week_end)):
                    continue
                candidate_result = _match_session_on_day(by_day.get(candidate_day, []), log.session_name)
                candidate_km = candidate_result[1]
                if candidate_km <= 0:
                    continue
                if best_day is None or candidate_km > best_result[1]:
                    best_day = candidate_day
                    best_result = candidate_result
            if best_day is not None:
                matched_run, run_km, run_pace, matched_items = best_result
                moved_status = True
                moved_note = f"Moved from {log.workout_date.strftime('%a')} to {best_day.strftime('%a')}"

        new_status = log.status
        new_actual = log.actual_distance_km
        new_notes = log.notes
        if log.workout_type == "RUN":
            target = float(log.target_distance_km or 0.0)
            if run_km > 0:
                if log.session_name in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"}:
                    base_status, _, _ = _classify_quality_completion(log.session_name, run_km, target, run_pace, weekly_goal)
                else:
                    base_status, _, _ = _classify_run_completion(run_km, target)
                new_status = "moved" if moved_status and base_status in {"completed", "partial"} else base_status
                new_actual = run_km
                new_notes = moved_note or log.notes
                for item in matched_items:
                    if getattr(item, "strava_activity_id", None) is not None:
                        used_activity_ids.add(item.strava_activity_id)
            elif log.workout_date < today_local:
                new_status = "missed"
                new_actual = None
            else:
                new_status = "planned"
                new_actual = None
        elif log.workout_type == "STRENGTH":
            if strength_done:
                new_status = "completed"
            elif log.workout_date < today_local:
                new_status = "missed"
            else:
                new_status = "planned"
                new_actual = None
        else:
            new_status = "planned"

        computed[log.workout_date] = (new_status, new_actual, new_notes)

    if persist:
        for log in logs:
            new_status, new_actual, new_notes = computed.get(log.workout_date, (log.status, log.actual_distance_km, log.notes))
            if new_status != log.status or new_actual != log.actual_distance_km or new_notes != log.notes:
                upsert_workout_log(
                    user_id=user_id,
                    workout_date=log.workout_date,
                    workout_type=log.workout_type,
                    session_name=log.session_name,
                    target_distance_km=log.target_distance_km,
                    status=new_status,
                    actual_distance_km=new_actual,
                    notes=new_notes,
                    source=log.source,
                    auto_commit=False,
                )
        commit_all()

    out = []
    for log in fetch_workout_logs(user_id, week_start, week_end):
        planned_day = template[log.workout_date.weekday()]
        planned_km = round(float(log.target_distance_km or 0.0), 1) if log.target_distance_km else None
        actual_km = round(float(log.actual_distance_km or 0.0), 1) if log.actual_distance_km is not None else None
        completion_pct = None
        extra_km = 0.0
        if log.workout_type == "RUN" and planned_km and actual_km is not None:
            _, completion_pct, extra_km = _classify_run_completion(actual_km, planned_km)
        out.append({
            "day": log.workout_date.strftime("%a"),
            "date": log.workout_date,
            "session": log.session_name,
            "planned": f"{int(round(planned_km))} km" if planned_km else ("Gym" if log.workout_type == "STRENGTH" else "Rest"),
            "planned_km": planned_km,
            "actual": f"{actual_km} km" if actual_km is not None else ("Gym" if log.status == "completed" and log.workout_type == "STRENGTH" else None),
            "actual_km": actual_km,
            "done": log.status == "completed",
            "moved": log.status == "moved",
            "status": log.status,
            "status_label": _status_label(log.status),
            "completion_pct": completion_pct,
            "extra_km": extra_km,
            "note": log.notes,
            "workout_type": log.workout_type,
            "intensity": planned_day.get("intensity"),
            "importance": planned_day.get("importance"),
            "purpose": planned_day.get("purpose"),
        })
    planned_run_goal_km = round(sum(float(item.get("planned_km") or 0.0) for item in out if item["workout_type"] == "RUN" and item["session"] != "Race Day"), 1)
    adaptive_weekly_goal = dict(weekly_goal)
    adaptive_weekly_goal["weekly_goal_km"] = max(float(weekly_goal.get("weekly_goal_km") or 0.0), planned_run_goal_km)
    adaptive_weekly_goal["max_safe_run"] = max(
        float(weekly_goal.get("max_safe_run") or 0.0),
        round(adaptive_weekly_goal["weekly_goal_km"] * 0.35, 1),
    )
    out = _apply_adaptive_plan(out, today_local, adaptive_weekly_goal)

    for item in out:
        if item["date"] >= today_local and item["status"] == "planned":
            upsert_workout_log(
                user_id=user_id,
                workout_date=item["date"],
                workout_type=item["workout_type"],
                session_name=item["session"],
                target_distance_km=item.get("planned_km"),
                status=item["status"],
                actual_distance_km=item.get("actual_km"),
                notes=item.get("adaptive_note"),
                source="engine",
                auto_commit=False,
            )
    commit_all()
    return out


def _pick_key_session(today_local, weekly_plan):
    upcoming = [w for w in weekly_plan if w["date"] >= today_local and w["status"] in {"planned", "partial", "missed"}]
    if not upcoming:
        return weekly_plan[-1] if weekly_plan else None

    return min(upcoming, key=_priority_rank)


def _next_upcoming_run_label(today_local, weekly_plan):
    for item in weekly_plan:
        if item["workout_type"] != "RUN":
            continue
        if item["date"] > today_local and item["status"] == "planned":
            return f"{item['day']} - {item['planned']} {item['session']}"
    return "No upcoming run scheduled this week."


def _next_upcoming_run_from_plan(today_local, current_plan, next_week_plan=None):
    current_next = _next_upcoming_run_label(today_local, current_plan)
    if current_next != "No upcoming run scheduled this week.":
        return current_next

    next_week_plan = next_week_plan or []
    future_candidates = [
        item for item in next_week_plan
        if item["workout_type"] == "RUN" and item["status"] == "planned"
    ]
    if not future_candidates:
        return current_next

    high_priority = [item for item in future_candidates if item["session"] in {"Race Day", "Long Run", "Marathon Pace Run", "Speed Session", "Tempo Run", "Medium Long Run", "Aerobic Run"}]
    selected = high_priority[0] if high_priority else future_candidates[0]
    return f"{selected['day']} - {selected['planned']} {selected['session']}"


def _build_today_workout(today_local, runs, weekly_plan, upcoming_run):
    today_iso = today_local.isoformat()
    today_run = next((r for r in runs if r["date"] == today_iso), None)
    today_assignment = next((w for w in weekly_plan if w["date"].isoformat() == today_iso), None)

    workout_name = today_assignment["session"] if today_assignment else "Rest"
    workout_type = today_assignment["workout_type"] if today_assignment else "REST"
    target = today_assignment["planned"] if today_assignment else "Rest"
    planned_km = float(today_assignment["planned_km"] or 0.0) if today_assignment else 0.0

    # Compute tomorrow's planned session for the "Tomorrow" field.
    tomorrow_local = today_local + timedelta(days=1)
    tomorrow_plan = next((w for w in weekly_plan if w["date"] == tomorrow_local), None)
    if tomorrow_plan:
        if tomorrow_plan["workout_type"] == "STRENGTH":
            tomorrow_label = f"Gym — {tomorrow_plan['session']}"
        elif tomorrow_plan["workout_type"] == "RUN":
            tomorrow_label = f"{tomorrow_plan['planned']} {tomorrow_plan['session']}"
        else:
            tomorrow_label = "Rest"
    else:
        tomorrow_label = "Rest"

    if today_run:
        actual_km = float(today_run["distance"])
        status_key, completion_pct, extra_km = _classify_run_completion(actual_km, planned_km)
        return {
            "date": today_local.strftime("%a %b %d"),
            "workout": workout_name,
            "workout_type": workout_type,
            "status": _status_label(status_key),
            "distance_target": target,
            "distance": f"{today_run['distance']} km",
            "distance_actual": f"{today_run['distance']} km",
            "completion_pct": completion_pct,
            "extra_distance": f"+{extra_km} km" if extra_km > 0 else None,
            "pace": today_run["pace"],
            "hr": str(today_run["hr"]) if today_run["hr"] else "--",
            "coach_insight": "Workout completed for today. Keep recovery and hydration on track.",
            "tomorrow": tomorrow_label,
            "completed": True,
        }

    return {
        "date": today_local.strftime("%a %b %d"),
        "workout": workout_name,
        "workout_type": workout_type,
        "status": "Upcoming",
        "distance_target": target,
        "distance": "--",
        "distance_actual": "--",
        "completion_pct": None,
        "extra_distance": None,
        "pace": "--",
        "hr": "--",
        "coach_insight": "Today's session is assigned from your weekly plan.",
        "tomorrow": tomorrow_label,
        "completed": False,
    }


def _build_ai_summary(intel, weekly_plan):
    # Priority: fatigue/injury risk -> missed key work -> long run readiness -> specificity -> prediction readiness
    bonk_label = (intel.get("bonk_risk", {}).get("label") or "").lower()
    next_run = next((u for u in weekly_plan if u.get("workout_type") == "RUN" and u.get("status") == "planned"), None)
    fatigue_flags = intel.get("fatigue_flags", {})
    next_key = next((u for u in weekly_plan if u.get("workout_type") == "RUN" and u.get("status") == "planned" and u.get("session") in {"Race Day", "Long Run", "Marathon Pace Run", "Speed Session", "Tempo Run"}), next_run)
    if intel.get("weekly", {}).get("phase") == "taper":
        if next_key:
            return f"Taper week now. Keep {next_key['day']} {next_key['session']} controlled and protect freshness for race day."
        return "Taper week now. Keep the effort light and arrive at race day fresh."
    if fatigue_flags.get("high_fatigue"):
        if next_key:
            return f"Fatigue is elevated. Convert the focus to recovery, then keep {next_key['day']} {next_key['session']} controlled."
        return "Fatigue is elevated. Favor recovery and avoid adding extra intensity."
    if bonk_label == "high":
        next_long = next((u for u in weekly_plan if u.get("session") == "Long Run" and u.get("status") == "planned"), None)
        if next_long:
            return f"Bonk risk is still high. Prioritize {next_long['day']} {next_long['planned']} {next_long['session']} and keep fueling practice consistent."
        if next_run:
            return f"Keep the next step simple. Complete {next_run['day']} {next_run['session']} to reduce fatigue risk."
        return "Keep the next step simple. Prioritize your next aerobic run and protect recovery."

    skipped_runs = [p for p in weekly_plan if p.get("status") in {"missed", "skipped"} and p.get("workout_type") == "RUN"]
    planned_runs = [p for p in weekly_plan if p.get("workout_type") == "RUN"]
    completed_runs = [p for p in planned_runs if p.get("status") == "completed"]
    upcoming_runs = [p for p in weekly_plan if p.get("workout_type") == "RUN" and p.get("status") == "planned"]

    if len(skipped_runs) >= 2:
        consistency_pct = int(round((len(completed_runs) / max(1, len(planned_runs))) * 100))
        return f"You skipped {len(skipped_runs)} workouts this week. Training consistency dropped to {consistency_pct}%."

    if skipped_runs:
        missed = skipped_runs[0]
        next_two = ", ".join([f"{u['day']} {u['session']}" for u in upcoming_runs[:2]])
        if next_two:
            return f"You skipped {missed['day']}'s {missed['session']}. Focus on {next_two}."
        return f"You skipped {missed['day']}'s {missed['session']}. Complete your next scheduled run to recover consistency."

    long_run_count = intel.get("training_counts", {}).get("long_runs", 0)
    if long_run_count < 2:
        next_long = next((u for u in upcoming_runs if u.get("session") == "Long Run"), None)
        if next_long:
            return f"Long-run readiness is building. Prioritize {next_long['day']} {next_long['planned']} {next_long['session']}."
        return "Long-run readiness is building. Schedule a qualifying long run this weekend."

    if intel.get("marathon_specificity_pct", 0) < 60:
        next_specific = next((u for u in upcoming_runs if u.get("session") == "Marathon Pace Run"), None)
        if next_specific:
            return f"Marathon specificity is still building. Hit {next_specific['day']} {next_specific['planned']} {next_specific['session']} at controlled goal pace."

    next_marathon_specific = next((u for u in upcoming_runs if u.get("session") == "Marathon Pace Run"), None)
    if next_marathon_specific:
        return f"Your next key specificity session is {next_marathon_specific['day']} {next_marathon_specific['planned']} {next_marathon_specific['session']}. Lock into goal pace and keep it controlled."

    trend = (intel.get("fitness_trend_label") or "").lower()
    if trend == "declining":
        next_one = next((u for u in upcoming_runs), None)
        if next_one:
            return f"Training momentum is down. Complete {next_one['day']} {next_one['session']} to stabilize load."
        return "Training momentum is down. Complete your next planned run to rebuild consistency."

    next_req = intel.get("training_status", {}).get("next_requirement")
    if next_req:
        return next_req

    next_long = next((u for u in upcoming_runs if u.get("session") == "Long Run"), None)
    if next_long:
        return f"Training is on track. Nail {next_long['day']} {next_long['planned']} {next_long['session']} this week."
    if next_run:
        return f"Complete {next_run['day']} {next_run['session']} to keep weekly mileage on track."
    return "Training is on track. Stay consistent with your weekly plan."


def _training_consistency_score(user_id, today_local):
    start = today_local - timedelta(days=27)
    logs = fetch_workout_logs(user_id, start, today_local)
    return service_training_consistency_score(logs)
