import json
import copy
import math
import re
import secrets
import smtplib
from datetime import date, datetime, timedelta, timezone
from email.mime.text import MIMEText
from functools import wraps
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests as http_requests
from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session, url_for
from flask_limiter.util import get_remote_address
from werkzeug.security import check_password_hash, generate_password_hash

from .repositories import (
    commit_all,
    consume_password_reset,
    create_password_reset,
    create_user,
    delete_workout_log,
    fetch_activities_between,
    fetch_recent_predictions,
    fetch_workout_logs,
    get_goal,
    get_password_reset,
    get_user_by_email,
    get_user_by_id,
    save_goal,
    update_password,
    update_user_name,
    upsert_workout_log,
)
from .services.analytics_service import (
    performance_intelligence,
    recent_runs,
    weekly_training_summary,
)
from .services.plan_engine import (
    apply_adaptive_plan as service_apply_adaptive_plan,
    build_progression_weeks as service_build_progression_weeks,
    build_weekly_plan_template,
    classify_quality_completion as service_classify_quality_completion,
    effective_long_run_base_km as service_effective_long_run_base_km,
    classify_run_completion as service_classify_run_completion,
    goal_marathon_pace as service_goal_marathon_pace,
    plan_meta_for_session as service_plan_meta_for_session,
    prescribed_long_run_km as service_prescribed_long_run_km,
    quality_session_prescription as service_quality_session_prescription,
    select_best_run_for_session as service_select_best_run_for_session,
    training_consistency_score as service_training_consistency_score,
)
from .services.strava_oauth_service import (
    exchange_code_for_token,
    generate_oauth_state,
    get_authorize_url,
    link_oauth_identity,
)
from .services.ai_recommendation_service import build_pace_strategy, generate_coaching_output
from .services.load_engine import running_stress_score as _rss_fn, STRESS_TYPE_FACTOR as _STRESS_TYPE_FACTOR
from .services.prediction_engine import vdot_from_race, vdot_to_race_time_seconds
from .services.strava_service import sync_strava_data
from .services.data_quality import DataQualityReport
from .services.ai_coach_engine import AICoachEngine
from .services.training_state_engine import (
    CROSS_TRAIN,
    DIFFERENT_ACTIVITY,
    DONE,
    MISSED,
    OVERDONE,
    PARTIAL,
    PLANNED,
    REST,
    RUN,
    SKIPPED,
    STRENGTH,
    TODAY,
    WALK,
    aggregate_actual_activities,
    build_week_plan_state,
    build_weekly_plan_snapshot,
    compute_week_metrics,
    today_session_from_plan,
    weekly_snapshot_is_valid,
)
from .models import Activity, CoachingPlan, Goal, Metric, PredictionHistory, RunnerProfile, StravaToken, WorkoutLog
from .extensions import csrf, db, limiter

web = Blueprint("web", __name__)


@web.route("/healthz")
def healthz():
    return {"status": "ok"}, 200


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _user_timezone_name():
    return current_app.config.get("USER_TIMEZONE") or current_app.config.get("APP_TIMEZONE") or "Asia/Kolkata"

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


def _apply_confirmed_current_week_repair(user_id, plan_row, week_start, snapshot):
    if not plan_row or not snapshot:
        return snapshot, False
    if week_start != date(2026, 3, 23):
        return snapshot, False
    if snapshot.get("manual_repair_applied"):
        return snapshot, False
    try:
        days = snapshot.get("days") or {}
        current_signature = {
            "weekly_target_km": int(round(float(snapshot.get("weekly_target_km") or 0.0))),
            "monday": int(round(float((days.get("2026-03-23", {}) or {}).get("planned_distance_km") or 0.0))),
            "tuesday": int(round(float((days.get("2026-03-24", {}) or {}).get("planned_distance_km") or 0.0))),
            "thursday": int(round(float((days.get("2026-03-26", {}) or {}).get("planned_distance_km") or 0.0))),
            "saturday": int(round(float((days.get("2026-03-28", {}) or {}).get("planned_distance_km") or 0.0))),
            "sunday": int(round(float((days.get("2026-03-29", {}) or {}).get("planned_distance_km") or 0.0))),
        }
    except Exception:
        return snapshot, False

    confirmed_signature = {
        "weekly_target_km": 43,
        "monday": 7,
        "tuesday": 5,
        "thursday": 9,
        "saturday": 7,
        "sunday": 15,
    }
    if current_signature == confirmed_signature:
        return snapshot, False

    repaired_daily_plan = {
        "monday": {"type": "easy", "km": 7, "notes": ""},
        "tuesday": {"type": "aerobic", "km": 5, "notes": ""},
        "wednesday": {"type": "strength", "km": 0, "notes": ""},
        "thursday": {"type": "easy", "km": 9, "notes": ""},
        "friday": {"type": "strength", "km": 0, "notes": ""},
        "saturday": {"type": "recovery", "km": 7, "notes": ""},
        "sunday": {"type": "long", "km": 15, "notes": ""},
    }
    repaired_snapshot = _replace_weekly_snapshot(plan_row, week_start, repaired_daily_plan)
    repaired_snapshot["manual_repair_applied"] = True

    freeze_state = _load_coaching_freeze_state(plan_row)
    snapshots = freeze_state.setdefault("weekly_plan_snapshots", {})
    snapshots[week_start.isoformat()] = repaired_snapshot
    _save_coaching_freeze_state(plan_row, freeze_state)

    existing_logs = {log.workout_date: log for log in fetch_workout_logs(user_id, week_start, week_start + timedelta(days=6))}
    for day_key, planned in repaired_snapshot.get("days", {}).items():
        day_date = date.fromisoformat(day_key)
        existing = existing_logs.get(day_date)
        status = existing.status if existing else "planned"
        actual_distance_km = existing.actual_distance_km if existing else None
        notes = existing.notes if existing else planned.get("notes", "")
        source = existing.source if existing else "engine"
        upsert_workout_log(
            user_id=user_id,
            workout_date=day_date,
            workout_type=planned["workout_type"],
            session_name=planned["session_name"],
            target_distance_km=planned["planned_distance_km"] if planned["planned_distance_km"] > 0 else None,
            status=status,
            actual_distance_km=actual_distance_km,
            notes=notes,
            source=source,
            auto_commit=False,
        )
    commit_all()
    return repaired_snapshot, True


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


def _display_planned_km(km):
    return int(round(float(km or 0.0))) if float(km or 0.0) > 0 else 0


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


def _different_activity_status_label(item, is_today=False):
    if item.get("workout_type") == "RUN":
        return "run open" if is_today else "run missed"
    if item.get("workout_type") == "STRENGTH":
        return "gym open" if is_today else "gym missed"
    return "other activity"


def _deterministic_progression_weeks(intel, week_start, current_week_weekly_target_km, current_week_long_run_km, weeks=18, schedule_prefs=None):
    weekly = intel.get("weekly") or {}
    goal = intel.get("goal") or {}
    long_run = intel.get("long_run") or {}
    schedule_prefs = dict(schedule_prefs or {})
    race_date = goal.get("race_date") or weekly.get("race_date")
    try:
        race_date_obj = datetime.fromisoformat(str(race_date)[:10]).date() if race_date else None
    except Exception:
        race_date_obj = None

    weekly_goal = {
        "weekly_goal_km": float(current_week_weekly_target_km or weekly.get("weekly_goal_km") or 0.0),
        "anchor_weekly_target_km": float(current_week_weekly_target_km or weekly.get("weekly_goal_km") or 0.0),
        "anchor_long_run_km": float(current_week_long_run_km or 0.0),
        "phase": weekly.get("phase") or weekly.get("display_phase") or "base",
        "rebuild_mode": bool(weekly.get("rebuild_mode")),
        "weeks_to_race": float(weekly.get("weeks_to_race") or max(0.0, float(goal.get("days_remaining") or 0.0) / 7.0)),
        "race_distance_km": float(weekly.get("race_distance_km") or goal.get("distance_km") or 42.195),
        "goal_marathon_pace_sec_per_km": float(weekly.get("goal_marathon_pace_sec_per_km") or 0.0),
        "prior_avg_km": float(weekly.get("prior_avg_km") or 0.0),
        "recent_avg_km": float(weekly.get("recent_avg_km") or 0.0),
        "training_consistency_ratio": float(weekly.get("training_consistency_ratio") or 0.0),
        "high_fatigue": bool(weekly.get("high_fatigue")),
        "moderate_fatigue": bool(weekly.get("moderate_fatigue")),
        "atl_spike": bool(weekly.get("atl_spike")),
        "week_start": week_start.isoformat(),
        "training_days_per_week": int(schedule_prefs.get("training_days_per_week") or 5),
        "long_run_day": str(schedule_prefs.get("long_run_day") or "sunday").lower(),
        "strength_days_per_week": int(schedule_prefs.get("strength_days_per_week") or 2),
    }
    effective_longest = service_effective_long_run_base_km(
        {
            "current_week_long_km": float(current_week_long_run_km or 0.0),
            "latest_km": float(long_run.get("latest_km") or 0.0),
            "latest_date": long_run.get("latest_date"),
            "longest_km": float(long_run.get("longest_km") or 0.0),
            "longest_date": long_run.get("longest_date"),
        },
        week_start,
    )
    long_run_state = {
        "longest_km": effective_longest,
        "effective_longest_km": effective_longest,
        "next_milestone_km": float(long_run.get("next_milestone_km") or 0.0),
        "latest_km": float(long_run.get("latest_km") or 0.0),
        "latest_date": long_run.get("latest_date"),
        "current_week_long_km": float(current_week_long_run_km or 0.0),
    }
    progression = service_build_progression_weeks(weekly_goal, long_run_state, weeks=weeks)
    return progression, weekly_goal, race_date_obj


def _deterministic_long_run_progression(intel, week_start, current_week_weekly_target_km, current_week_long_run_km, schedule_prefs=None):
    progression, weekly_goal, race_date_obj = _deterministic_progression_weeks(
        intel,
        week_start,
        current_week_weekly_target_km,
        current_week_long_run_km,
        weeks=18,
        schedule_prefs=schedule_prefs,
    )
    output = []
    for week in progression[1:]:
        week_date = week["week_start"] + timedelta(days=6)
        if race_date_obj and week_date >= race_date_obj:
            break
        target_km = _display_planned_km(week["long_run_km"])
        target_km = service_prescribed_long_run_km(
            target_km,
            phase=week["phase"],
            race_distance_km=weekly_goal["race_distance_km"],
        )
        output.append(
            {
                "week_date": week_date.isoformat(),
                "week_date_display": f"{week_date.strftime('%a')} {week_date.day} {week_date.strftime('%b')}",
                "target_km": target_km,
                "is_recovery_week": week["week_type"] in {"cutback", "recovery", "rebuild"},
                "is_peak_run": week["phase"] == "peak" and target_km >= 28,
                "label": (
                    "Peak phase"
                    if week["phase"] == "peak"
                    else "Base building"
                    if week["phase"] == "base"
                    else "Build phase"
                    if week["phase"] == "build"
                    else "Taper"
                    if week["phase"] == "taper"
                    else week["phase"].title()
                ),
                "week_type": week["week_type"],
                "variant_name": (week.get("long_run_variant") or {}).get("name"),
                "variant_short_label": (week.get("long_run_variant") or {}).get("short_label"),
                "variant_note": (week.get("long_run_variant") or {}).get("note"),
                "variant_pace_guidance": (week.get("long_run_variant") or {}).get("pace_guidance"),
                "variant_quality_type": (week.get("long_run_variant") or {}).get("quality_type"),
                "quality_block_km": (week.get("long_run_variant") or {}).get("quality_block_km", 0),
            }
        )
    return output


def _deterministic_future_week_preview(intel, week_start, current_week_weekly_target_km, current_week_long_run_km, limit=3, schedule_prefs=None):
    progression, weekly_goal, race_date_obj = _deterministic_progression_weeks(
        intel,
        week_start,
        current_week_weekly_target_km,
        current_week_long_run_km,
        weeks=max(6, limit + 2),
        schedule_prefs=schedule_prefs,
    )
    preview = []
    for week in progression[1:]:
        week_end = week["week_start"] + timedelta(days=6)
        if race_date_obj and week["week_start"] >= race_date_obj:
            break
        if race_date_obj and week_end >= race_date_obj:
            break
        template = week.get("template") or {}
        run_days = [day for day in template.values() if day.get("workout_type") == "RUN"]
        weekly_target = sum(_display_planned_km(day.get("target_km") or 0.0) for day in run_days)
        quality_day = next(
            (
                day for day in run_days
                if day.get("session") in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"}
            ),
            None,
        )
        medium_long_day = next((day for day in run_days if day.get("session") == "Medium Long Run"), None)
        long_day = next((day for day in run_days if day.get("session") == "Long Run"), None)
        snapped_long_run_km = service_prescribed_long_run_km(
            _display_planned_km(long_day.get("target_km") or 0.0) if long_day else 0,
            phase=week["phase"],
            race_distance_km=weekly_goal["race_distance_km"],
        )
        weekly_target = sum(_display_planned_km(day.get("target_km") or 0.0) for day in run_days)
        if long_day:
            weekly_target = weekly_target - _display_planned_km(long_day.get("target_km") or 0.0) + snapped_long_run_km
        week_label = f"{week['week_start'].strftime('%d %b')} - {week_end.strftime('%d %b')}"
        quality_prescription = (
            service_quality_session_prescription(
                quality_day.get("session"),
                _display_planned_km(quality_day.get("target_km") or 0.0),
                {
                    **weekly_goal,
                    "phase": week["phase"],
                    "progression_week_type": week["week_type"],
                },
            )
            if quality_day
            else None
        )
        preview.append(
            {
                "week_label": week_label,
                "phase_label": (
                    "Base build"
                    if week["phase"] == "base"
                    else "Build"
                    if week["phase"] == "build"
                    else "Peak"
                    if week["phase"] == "peak"
                    else "Taper"
                    if week["phase"] == "taper"
                    else week["phase"].title()
                ),
                "week_type": week["week_type"],
                "weekly_target_km": weekly_target,
                "quality_session": {
                    "name": quality_day.get("session"),
                    "km": _display_planned_km(quality_day.get("target_km") or 0.0),
                    "detail": quality_prescription.get("structure_summary"),
                    "pace_guidance": quality_prescription.get("pace_guidance"),
                } if quality_day else None,
                "medium_long_session": {
                    "name": medium_long_day.get("session"),
                    "km": _display_planned_km(medium_long_day.get("target_km") or 0.0),
                    "detail": medium_long_day.get("purpose") or medium_long_day.get("intensity"),
                } if medium_long_day else None,
                "long_run_session": {
                    "name": long_day.get("session"),
                    "km": snapped_long_run_km,
                    "detail": ((week.get("long_run_variant") or {}).get("short_label") or "Long run"),
                } if long_day else None,
            }
        )
        if len(preview) >= limit:
            break
    return preview


def _build_upcoming_long_runs(current_week_plan, future_progression, today_local, limit=4):
    upcoming = []
    current_long_run = next(
        (
            item for item in current_week_plan
            if item.get("workout_type") == "RUN"
            and item.get("session") == "Long Run"
            and item.get("day_date") > today_local
            and item.get("status") in {"planned", "today"}
        ),
        None,
    )
    if current_long_run:
        day_date = current_long_run["day_date"]
        upcoming.append(
            {
                "week_date": day_date.isoformat(),
                "week_date_display": f"{day_date.strftime('%a')} {day_date.day} {day_date.strftime('%b')}",
                "target_km": int(current_long_run.get("display_planned_km") or 0),
                "is_recovery_week": False,
                "is_peak_run": False,
                "label": "This week",
                "week_type": "current",
                "variant_name": current_long_run.get("session"),
                "variant_short_label": "Long run",
                "variant_note": current_long_run.get("notes"),
                "variant_pace_guidance": current_long_run.get("pace_guidance"),
                "variant_quality_type": "easy",
                "quality_block_km": 0,
            }
        )

    for item in future_progression:
        if item.get("week_date", "") <= today_local.isoformat():
            continue
        upcoming.append(item)
        if len(upcoming) >= limit:
            break
    return upcoming[:limit]


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
        summary = (
            f"You've completed {actual_km:.1f} km of {weekly_target:.0f} km this week. "
            f"Keep building toward a {long_run_target:.0f} km long run and stay consistent over the next {max(1, days_remaining // 7)} weeks."
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
    for day_key, planned in snapshot.get("days", {}).items():
        day_date = date.fromisoformat(day_key)
        if WorkoutLog.query.filter_by(user_id=user_id, workout_date=day_date).first():
            continue
        upsert_workout_log(
            user_id=user_id,
            workout_date=day_date,
            workout_type=planned["workout_type"],
            session_name=planned["session_name"],
            target_distance_km=planned["planned_distance_km"] if planned["planned_distance_km"] > 0 else None,
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

    Handles ranges like "25-28km" or "25 to 28km", single values with weekly
    context, targeting phrases, and long-run mentions.
    """
    # Match ranges like "25-28km" or "25 to 28km" near weekly context
    msg = re.sub(
        r'\b\d+\.?\d*\s*(?:to|-)\s*\d+\.?\d*\s*km'
        r'(?:\s*(?:per week|weekly|target|a week))?',
        f'{weekly_target:.0f}km',
        msg, flags=re.IGNORECASE
    )
    # Match single values like "25km target" or "25km per week"
    msg = re.sub(
        r'\b\d+\.?\d*\s*km\s*'
        r'(?:per week|weekly|target|a week|this week)',
        f'{weekly_target:.0f}km this week',
        msg, flags=re.IGNORECASE
    )
    # Match "targeting Xkm" or "target of Xkm"
    msg = re.sub(
        r'(?:target(?:ing)?(?:\s+of)?)\s+\d+\.?\d*\s*km',
        f'targeting {weekly_target:.0f}km',
        msg, flags=re.IGNORECASE
    )
    # Match long run mentions (including ranges)
    msg = re.sub(
        r'(?:long run|long-run)\s+(?:of\s+)'
        r'(?:\d+\.?\d*\s*(?:to|-)\s*)?\d+\.?\d*\s*km',
        f'long run of {long_run:.0f}km',
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


def _current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return get_user_by_id(user_id)


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _current_user():
            return redirect(url_for("web.login"))
        return fn(*args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# /api/dashboard — structured JSON coaching output (login required)
#
# Explicit pipeline:
#   1. load_engine  — fresh ATL/CTL/TSB via performance_intelligence
#   2. prediction   — TSB-adjusted race time (embedded in intel)
#   3. coaching     — all 3 structured sections via generate_coaching_output
# ---------------------------------------------------------------------------

@web.route("/api/dashboard")
@csrf.exempt  # JSON endpoint called by JS fetch — session auth enforces identity
@login_required
def api_dashboard():
    user = _current_user()
    user_tz = _user_timezone_name()
    today_local = _today_local_date(user_tz)

    # ── Step 1: Load engine ─────────────────────────────────────────────────
    # performance_intelligence → _metrics_layer → load_model (load_engine)
    # computes fresh ATL/CTL/TSB and embeds tsb_proxy into the metrics dict.
    intel = performance_intelligence(user.id, user_timezone=user_tz)
    if not intel or not intel.get("goal"):
        return jsonify({"error": "no_goal", "message": "Complete onboarding first."}), 400

    load_output = {
        "ctl": intel["current_ctl"],
        "atl": intel["current_atl"],
        "tsb": intel["current_tsb"],
        "fitness_trend": intel["fitness_trend_label"],
        "fatigue_ratio": intel.get("fatigue_flags", {}).get("fatigue_ratio", 1.0),
        "ctl_series": intel.get("charts", {}).get("ctl_14", []),
    }

    # ── Step 2: Prediction engine ───────────────────────────────────────────
    # marathon_prediction_seconds already received tsb_proxy from step 1
    # (passed through the metrics dict inside performance_intelligence).
    prediction_output = {
        "predicted_time": intel.get("race_day_projection", "--"),
        "current_fitness_time": intel.get("current_projection", "--"),
        "confidence": intel.get("prediction_confidence", "Low"),
        "confidence_score": intel.get("prediction_confidence_score", 0.0),
        "goal_time": (intel.get("goal") or {}).get("goal_time", "--"),
        "gap_to_goal": intel.get("gap_to_goal", "--"),
        "probability": intel.get("probability"),
        "goal_alignment": intel.get("goal_alignment", "--"),
        "note": intel.get("prediction_note", ""),
    }

    # ── Step 3: Coaching output ─────────────────────────────────────────────
    # generate_coaching_output receives intel (containing both load and
    # prediction data) and the weekly plan for context.
    week_start, _ = _week_bounds(today_local)
    weekly_plan = _build_weekly_plan(
        user.id, today_local, user_tz,
        intel["weekly"], intel["long_run"],
        week_start=week_start,
        persist=False,
    )
    coaching = generate_coaching_output(intel, weekly_plan)

    # ── Step 4: Single JSON response with all 3 outputs ─────────────────────
    return jsonify({
        "load": load_output,
        "prediction": prediction_output,
        "pace_strategy": coaching["pace_strategy"],
        "training_recommendations": coaching["training_recommendations"],
        "coaching_summary": coaching["coaching_summary"],
        "wall_analysis": coaching["wall_analysis"],
    })


# ---------------------------------------------------------------------------
# /api/predict — standalone prediction (no auth required)
#
# Accepts a single recent race result and target distance; returns predicted
# time, VDOT, pace strategy for all 4 distances, and confidence score.
# ---------------------------------------------------------------------------

_TARGET_DISTANCES = {
    "5K":  5.0,
    "10K": 10.0,
    "HM":  21.0975,
    "FM":  42.195,
}


def _predict_confidence(source_km: float, target_km: float, weekly_km: float, vdot: float) -> tuple:
    """Return (confidence_score 0–1, label) for a standalone VDOT prediction.

    Factors:
      - Source/target distance ratio  (closer = more reliable Riegel/VDOT)
      - VDOT plausibility             (realistic range for human runners)
      - Weekly training volume        (proxy for current fitness validity)
    """
    score = 0.0

    ratio = target_km / max(source_km, 0.001)
    if ratio <= 1.5:
        score += 0.40   # e.g. 10K → HM, HM → FM
    elif ratio <= 3.0:
        score += 0.30   # e.g. 5K → HM
    elif ratio <= 6.0:
        score += 0.18   # e.g. 5K → FM (significant extrapolation)
    else:
        score += 0.08

    if 25.0 <= vdot <= 85.0:
        score += 0.30   # plausible recreational → elite range
    elif 20.0 <= vdot <= 90.0:
        score += 0.15

    if weekly_km >= 50:
        score += 0.30
    elif weekly_km >= 30:
        score += 0.20
    elif weekly_km >= 15:
        score += 0.10

    score = round(min(1.0, score), 2)
    label = "High" if score >= 0.65 else "Medium" if score >= 0.40 else "Low"
    return score, label


@web.route("/api/predict", methods=["POST"])
@csrf.exempt  # JSON endpoint called by JS fetch — session auth enforces identity
def api_predict():
    data = request.get_json(silent=True) or {}

    # ── Input validation ─────────────────────────────────────────────────────
    try:
        source_km = float(data["recent_race_distance_km"])
        source_sec = float(data["recent_race_time_seconds"])
    except (KeyError, TypeError, ValueError):
        return jsonify({
            "error": "missing_fields",
            "message": "recent_race_distance_km and recent_race_time_seconds are required.",
        }), 400

    raw_target = str(data.get("target_race_distance", "FM")).upper().strip()
    target_km = _TARGET_DISTANCES.get(raw_target)
    if target_km is None:
        return jsonify({
            "error": "invalid_target",
            "message": f"target_race_distance must be one of {list(_TARGET_DISTANCES)}.",
        }), 400

    if source_km <= 0 or source_sec <= 0:
        return jsonify({
            "error": "invalid_race",
            "message": "Race distance and time must be positive.",
        }), 400

    weekly_km = float(data.get("current_weekly_km") or 0.0)

    # ── VDOT from supplied race ──────────────────────────────────────────────
    vdot = vdot_from_race(source_km * 1000.0, source_sec / 60.0)
    if not vdot or vdot < 15:
        return jsonify({
            "error": "implausible_race",
            "message": "Supplied race result produces an implausible VDOT. Check distance and time.",
        }), 422

    # ── Project to target distance ───────────────────────────────────────────
    predicted_sec = vdot_to_race_time_seconds(vdot, target_km * 1000.0)
    if not predicted_sec:
        return jsonify({"error": "projection_failed"}), 500

    # ── Pace strategy (all 4 distances, anchored on marathon equivalent) ─────
    fm_sec = vdot_to_race_time_seconds(vdot, 42195.0)
    pace_strategy = build_pace_strategy(fm_sec)

    # ── Confidence ───────────────────────────────────────────────────────────
    confidence_score, confidence_label = _predict_confidence(source_km, target_km, weekly_km, vdot)

    h = int(predicted_sec // 3600)
    m = int((predicted_sec % 3600) // 60)
    s = int(round(predicted_sec % 60))

    return jsonify({
        "target_distance": raw_target,
        "predicted_time": f"{h}:{m:02d}:{s:02d}",
        "predicted_seconds": round(predicted_sec),
        "vdot": round(vdot, 1),
        "confidence_score": confidence_score,
        "confidence": confidence_label,
        "pace_strategy": pace_strategy,
        "source_race": {
            "distance_km": source_km,
            "time_seconds": round(source_sec),
        },
    })


def _send_reset_email(email, link):
    cfg = current_app.config
    if not cfg.get("SMTP_HOST"):
        print(f"[Password Reset Link] {email}: {link}")
        return

    msg = MIMEText(f"Reset your StrideIQ password: {link}")
    msg["Subject"] = "StrideIQ Password Reset"
    msg["From"] = cfg.get("SMTP_FROM")
    msg["To"] = email

    with smtplib.SMTP(cfg["SMTP_HOST"], cfg["SMTP_PORT"]) as server:
        server.starttls()
        if cfg.get("SMTP_USER") and cfg.get("SMTP_PASSWORD"):
            server.login(cfg["SMTP_USER"], cfg["SMTP_PASSWORD"])
        server.sendmail(msg["From"], [email], msg.as_string())


@web.route("/register", methods=["GET", "POST"])
@limiter.limit("10 per hour; 3 per minute")
def register():
    error = None
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not name or not email or len(password) < 6:
            error = "Please enter valid details (password min 6 chars)."
        elif get_user_by_email(email):
            error = "Email already exists. Please login."
        else:
            try:
                user_id = create_user(name, email, generate_password_hash(password))
                session["user_id"] = user_id
                return redirect(url_for("web.onboarding"))
            except Exception:
                error = "Unable to create account right now. Please try again."

    return render_template("register.html", error=error)


@web.route("/login", methods=["GET", "POST"])
@limiter.limit("20 per hour; 5 per minute")
def login():
    error = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        user = get_user_by_email(email)
        if not user or not check_password_hash(user.password_hash, password):
            error = "Invalid email or password"
        else:
            session["user_id"] = user.id
            return redirect(url_for("web.dashboard"))

    return render_template("login.html", error=error)


@web.route("/forgot-password", methods=["GET", "POST"])
@limiter.limit("5 per hour")
def forgot_password():
    message = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        user = get_user_by_email(email)
        if user:
            token = secrets.token_urlsafe(32)
            expires_at = _utcnow_naive() + timedelta(hours=1)
            create_password_reset(user.id, token, expires_at)
            link = url_for("web.reset_password", token=token, _external=True)
            _send_reset_email(email, link)

        message = "If this email exists, a password reset link has been sent."

    return render_template("forgot_password.html", message=message)


@web.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    row = get_password_reset(token)
    if not row or row.expires_at < _utcnow_naive():
        return redirect(url_for("web.login"))

    error = None
    if request.method == "POST":
        password = request.form.get("password", "")
        if len(password) < 6:
            error = "Password must be at least 6 characters."
        else:
            update_password(row.user_id, generate_password_hash(password))
            consume_password_reset(token)
            return redirect(url_for("web.login"))

    return render_template("reset_password.html", error=error)


@web.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("web.login"))


@web.route("/")
@login_required
@limiter.limit("4 per minute", key_func=lambda: f"{get_remote_address()}-sync", exempt_when=lambda: request.args.get("sync") != "1")
def dashboard():
    try:
        return _dashboard_inner()
    except Exception as _dash_exc:
        import traceback as _tb
        _trace = _tb.format_exc()
        current_app.logger.error("Dashboard 500: %s\n%s", _dash_exc, _trace)
        # TEMPORARY DEBUG — remove once root cause confirmed
        _safe = _trace.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        _html = (
            "<pre style='background:#0d1117;color:#f87171;padding:24px;"
            "font-family:monospace;font-size:13px;white-space:pre-wrap'>"
            "<b style='color:#fbbf24'>StrideIQ debug</b>\n\n"
            + _safe + "</pre>"
        )
        from flask import Response as _Resp
        return _Resp(_html, status=500, mimetype="text/html")


def _dashboard_inner():
    user = _current_user()
    user_tz = _user_timezone_name()

    # Gate: must complete coach onboarding before seeing dashboard
    _profile = RunnerProfile.query.filter_by(user_id=user.id).first()
    if not _profile or not _profile.onboarding_completed:
        _goal = Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first()
        if not _goal:
            return redirect(url_for("web.onboarding"))
        return redirect(url_for("web.coach_intro"))

    force_sync = request.args.get("sync") == "1"
    cooldown_min = int(current_app.config.get("STRAVA_SYNC_COOLDOWN_MIN", 15))
    last_sync_at = _parse_iso_datetime(session.get("last_sync_at"))

    if force_sync or _should_sync_now(last_sync_at, cooldown_min):
        sync_info = sync_strava_data(user_id=user.id, pages=current_app.config.get("STRAVA_FETCH_PAGES", 3))
        if sync_info.get("status") == "ok":
            session["last_sync_at"] = datetime.now(timezone.utc).isoformat()
    else:
        sync_info = {"status": "skipped", "reason": "cooldown", "new_activities": 0}

    # ── Data quality gate ────────────────────────────────────────────────────
    dq = DataQualityReport(user.id)
    if dq.confidence == "no_data":
        return render_template(
            "no_data.html",
            user=user,
            goal=Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first(),
            banner=dq.banner,
        )
    if not dq.is_sufficient:
        _goal = Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first()
        _runs = recent_runs(user.id, limit=10, user_timezone=user_tz)
        return render_template(
            "dashboard_limited.html",
            user=user,
            goal=_goal,
            dq=dq.to_dict(),
            banner=dq.banner,
            runs=_runs,
        )
    dq_report = dq.to_dict()

    # ── Intel (PMC / predictions / load) ─────────────────────────────────────
    intel = performance_intelligence(user.id, user_timezone=user_tz)

    if not intel or not intel.get("goal"):
        return redirect(url_for("web.onboarding"))

    # ── AI COACH ENGINE — Single Source of Truth ──────────────────────────────
    _coach = AICoachEngine()
    _coaching_plan = _coach.get_plan(user.id)

    canonical_phase_label          = _deterministic_phase_label(intel)
    canonical_long_run_km          = 0.0
    canonical_alerts               = _coaching_plan.get("alerts", [])
    canonical_week_theme           = _coaching_plan.get("week_theme", "")
    canonical_focus_point          = _coaching_plan.get("this_week", {}).get("focus_point", "")
    canonical_long_run_progression = []
    canonical_feasibility          = {}
    schedule_prefs                 = _schedule_preferences_from_profile(_profile)
    _daily_plan                    = _deterministic_current_week_daily_plan(
        intel,
        _week_bounds(_today_local_date(user_tz))[0],
        schedule_prefs=schedule_prefs,
    )
    _profile_data                  = _coaching_plan.get("runner_profile", {})
    canonical_long_run_day         = schedule_prefs.get("long_run_day", "sunday").title()
    today_local = _today_local_date(user_tz)
    week_start, week_end = _week_bounds(today_local)

    # Compute weekly target from the frozen weekly snapshot only.
    _WEEK_DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    _plan_row = _get_coaching_plan_row(user.id)
    weekly_snapshot = _load_or_create_weekly_snapshot(_plan_row, week_start, _daily_plan)
    if _snapshot_needs_schedule_repair(weekly_snapshot, _profile, week_start) and _plan_row:
        weekly_snapshot = _replace_weekly_snapshot(_plan_row, week_start, _daily_plan)
    weekly_snapshot, _ = _apply_confirmed_current_week_repair(user.id, _plan_row, week_start, weekly_snapshot)
    _persist_snapshot_workout_logs(user.id, weekly_snapshot)
    canonical_weekly_target_km = round(float(weekly_snapshot.get("weekly_target_km") or 0.0), 1)
    canonical_long_run_km = round(
        max(
            [
                float(day.get("planned_km") or 0.0)
                for day in (weekly_snapshot.get("days") or {}).values()
                if (day.get("session_name") or "") == "Long Run"
            ]
            or [0.0]
        ),
        1,
    )
    current_app.logger.debug(
        f"[weekly_target] frozen_snapshot week={week_start.isoformat()} target={canonical_weekly_target_km:.1f}"
    )

    # Post-process coaching message — replace stale km targets + fix AI grammar
    _raw_coaching_message = _coaching_plan.get("coaching_message", "")
    # Apply broader coaching number replacements (handles ranges like "25-28km")
    _raw_coaching_message = fix_coaching_numbers(
        _raw_coaching_message,
        canonical_weekly_target_km,
        canonical_long_run_km,
    )
    for _pat, _rep in [
        (r"\bjumping from\b", "building from"),
        (r"\bwe'll\b",        "you'll"),
        (r"\bwe need\b",      "you need"),
        (r"\bwe build\b",     "you build"),
        (r"\blet's\b",        "focus on"),
        (r"\bwe're\b",        "you're"),
    ]:
        _raw_coaching_message = re.sub(_pat, _rep, _raw_coaching_message, flags=re.IGNORECASE)
    canonical_coaching_message = _raw_coaching_message

    # ── Aerobic potential pace + wall math transparency ──────────────────────
    _wall_data = intel.get('wall_analysis') or {}
    _ap_seconds = _wall_data.get('optimal_fm_potential') or 0
    if _ap_seconds:
        _ap_pace_sec = _ap_seconds / 42.195
        aerobic_pace_display = f"{int(_ap_pace_sec // 60)}:{int(_ap_pace_sec % 60):02d}/km avg"
    else:
        aerobic_pace_display = ''

    # Wall factor math — compute canonical wall-adjusted prediction for the JS card
    _WALL_FACTORS = {'low': 1.00, 'medium': 1.03, 'high': 1.08, 'unknown': 1.05}
    _wall_risk = (_wall_data.get('wall_risk') or 'unknown').lower()
    _wall_factor = _WALL_FACTORS.get(_wall_risk, 1.05)
    _base_pred_seconds = float(intel.get('predictions', {}).get('marathon', {}).get('seconds', 0) or 0)

    if _wall_risk == 'unknown' or not _base_pred_seconds:
        show_wall_estimate = False
        canonical_wall_adjusted_seconds = 0
        canonical_wall_cost_minutes = 0
    else:
        show_wall_estimate = True
        canonical_wall_adjusted_seconds = round(_base_pred_seconds * _wall_factor)
        canonical_wall_cost_minutes = abs(round((_base_pred_seconds * _wall_factor - _base_pred_seconds) / 60, 1))

    def _fmt_hms(total_sec):
        total_sec = int(total_sec)
        h, rem = divmod(total_sec, 3600)
        m, s   = divmod(rem, 60)
        return f"{h}:{m:02d}:{s:02d}"

    canonical_wall_base_display     = _fmt_hms(_base_pred_seconds) if _base_pred_seconds else ''
    canonical_wall_adjusted_display = _fmt_hms(canonical_wall_adjusted_seconds) if canonical_wall_adjusted_seconds else ''

    # ── Goal seconds (needed for FM gap + CTL chart) ─────────────────────────
    _goal_obj  = intel.get("goal") or {}
    _goal_secs = float(_goal_obj.get("goal_seconds") or 0)
    _goal_dist = float(_goal_obj.get("distance_km") or 42.195)
    _mp        = _goal_secs / _goal_dist if _goal_secs > 0 else 339.0

    # ── FM goal gap ───────────────────────────────────────────────────────────
    _fm_pred_sec = canonical_wall_adjusted_seconds or _base_pred_seconds
    if _fm_pred_sec and _goal_secs:
        _gap_sec = _fm_pred_sec - _goal_secs
        _gap_min = abs(int(_gap_sec // 60))
        if _gap_sec > 30:
            fm_gap_display = f"+{_gap_min} min from goal"
            fm_gap_color   = "amber"
        elif _gap_sec < -30:
            fm_gap_display = f"{_gap_min} min under goal"
            fm_gap_color   = "green"
        else:
            fm_gap_display = "On goal pace"
            fm_gap_color   = "green"
    else:
        fm_gap_display = ""
        fm_gap_color   = "green"

    # ── Feasibility label with emoji ──────────────────────────────────────────
    _feasibility_emoji_map = {
        'on_track':       '🔥',
        'achievable':     '⚡',
        'at_risk':        '⚠️',
        'needs_revision': '🔴',
    }
    _raw_label    = canonical_feasibility.get("assessment_label", "")
    _assessment   = canonical_feasibility.get("assessment", "")
    _label_emoji  = _feasibility_emoji_map.get(_assessment, '')
    canonical_feasibility_label_emoji = f"{_raw_label} {_label_emoji}".strip() if _raw_label else ""

    # ── Wall risk actions ─────────────────────────────────────────────────────
    _readiness = canonical_feasibility.get("readiness", {})
    _curr_vol  = float(_readiness.get("current_weekly_avg_km", 0) or 0)
    if _wall_risk == 'high':
        wall_actions = [
            f"Complete {canonical_long_run_km:.0f} km long run this weekend",
            f"Build weekly volume to {min(_curr_vol * 1.1, 50):.0f} km/week" if _curr_vol else "Build weekly volume gradually",
            "Run all easy segments at conversational pace",
            "Practice race-day fueling on long runs",
        ]
    elif _wall_risk == 'medium':
        wall_actions = [
            "Maintain consistent long runs as scheduled",
            "Practice race-day fueling on long runs",
            "Run first 5 km of race conservatively",
        ]
    else:
        wall_actions = []

    # ── Fetch this week's activities ──────────────────────────────────────────

    # ── 8-week CTL series — replay load_engine formula so chart matches fitness card ──
    import math as _math
    _CTL_DECAY = _math.exp(-1.0 / 42.0)
    _CTL_GAIN  = 1.0 - _CTL_DECAY

    _all_acts_raw = (
        Activity.query
        .filter(Activity.user_id == user.id)
        .order_by(Activity.date.asc())
        .all()
    )
    _daily_tss_map: dict = {}
    for _a in _all_acts_raw:
        _d = float(_a.distance_km or 0)
        _t = float(_a.moving_time or 0)
        _a_type = (_a.activity_type or "").lower()
        # Apply the same filters as analytics_service._load_activities so the graph
        # matches the CTL card value from performance_intelligence():
        #   - only activity types in STRESS_TYPE_FACTOR
        #   - exclude races
        #   - exclude runs shorter than 3 km
        #   - exclude zero-duration activities
        #   - exclude implausible ultra distances
        if _a_type not in _STRESS_TYPE_FACTOR:
            continue
        if getattr(_a, "is_race", False):
            continue
        if _a_type in {"run", "trailrun"} and _d < 3.0:
            continue
        if _t <= 0:
            continue
        if _d and _d > 50.0:
            continue
        _a_date = _activity_local_date(_a.date, user_tz)
        _act_dict = {
            "type": _a_type,
            "moving_time_sec": _t,
            "distance_km": _d,
            "pace_sec_per_km": (_t / _d) if _d > 0 and _t > 0 else None,
            "elevation_gain": float(_a.elevation_gain) if _a.elevation_gain else 0.0,
        }
        _daily_tss_map[_a_date] = _daily_tss_map.get(_a_date, 0.0) + _rss_fn(_act_dict, _mp)

    _ctl_start = min(_daily_tss_map.keys()) if _daily_tss_map else today_local - timedelta(days=120)
    _ctl_start = min(_ctl_start, today_local - timedelta(days=120))
    _ctl_timeline: dict = {}
    _ctl_running = 0.0
    _cur = _ctl_start
    while _cur <= today_local:
        _tss = _daily_tss_map.get(_cur, 0.0)
        _ctl_running = _ctl_running * _CTL_DECAY + _tss * _CTL_GAIN
        _ctl_timeline[_cur] = round(_ctl_running, 1)
        _cur += timedelta(days=1)

    _eight_weeks_ago = today_local - timedelta(weeks=8)
    weekly_ctl_series = []
    for _wn in range(9):
        _ws = _eight_weeks_ago + timedelta(weeks=_wn)
        _we = min(_ws + timedelta(days=6), today_local)
        weekly_ctl_series.append({
            'week': f"{_ws.strftime('%b')} {_ws.day}",
            'ctl':  _ctl_timeline.get(_we, _ctl_timeline.get(_ws, 0.0)),
        })
    weekly_ctl_series = weekly_ctl_series[-8:]

    # Query a padded UTC window, then assign each activity to the user's local
    # date. This prevents local-day runs near midnight from being dropped before
    # local-date bucketing.
    week_cutoff_dt = datetime.combine(week_start - timedelta(days=1), datetime.min.time())
    week_end_dt    = datetime.combine(week_end + timedelta(days=1), datetime.max.time())

    # Activity type sets — confirmed from production Strava data
    _RUN_TYPES            = {"run", "virtualrun", "trail run", "trail_run", "treadmill", "track"}
    _STRENGTH_TYPES       = {"strength", "weight_training", "strength_training", "crossfit", "yoga", "pilates", "workout", "core", "flexibility"}
    _CROSS_TRAINING_TYPES = {"ride", "virtualride", "cycling", "swim", "swimming", "rowing", "elliptical", "stairstepper", "hiit", "aerobics"}
    _RECOVERY_TYPES       = {"walk", "hike", "stretching", "massage", "icebath", "meditation"}
    _ALL_TRACKED          = _RUN_TYPES | _STRENGTH_TYPES | _CROSS_TRAINING_TYPES | _RECOVERY_TYPES

    week_activities = (
        Activity.query
        .filter(
            Activity.user_id == user.id,
            Activity.date >= week_cutoff_dt,
            Activity.date <= week_end_dt,
        )
        .order_by(Activity.date.asc())
        .all()
    )

    # Bucket all week activities by date and type-category
    _acts_by_date: dict[date, list] = {}
    for _a in week_activities:
        _d = _a.date.date() if isinstance(_a.date, datetime) else _a.date
        _acts_by_date.setdefault(_d, []).append(_a)

    def _day_acts(d):
        return _acts_by_date.get(d, [])

    def _run_acts(acts):
        return [a for a in acts if (a.activity_type or "").lower() in _RUN_TYPES]

    actual_by_date = aggregate_actual_activities(
        week_activities,
        lambda dt_value: _activity_local_date(dt_value, user_tz),
    )
    week_plan_state = build_week_plan_state(weekly_snapshot, actual_by_date, today_local)
    week_metrics = compute_week_metrics(week_plan_state, actual_by_date)
    week_actual_km = week_metrics["actual_km"]

    _state_to_status = {
        DONE: "completed",
        OVERDONE: "overdone",
        PARTIAL: "partial",
        MISSED: "missed",
        SKIPPED: "skipped",
        DIFFERENT_ACTIVITY: "different_activity",
        PLANNED: "planned",
        TODAY: "today",
    }
    _persisted_status_map = {
        "completed": "completed",
        "overdone": "overdone",
        "partial": "partial",
        "missed": "missed",
        "skipped": "skipped",
        "different_activity": "skipped",
        "planned": "planned",
        "today": "planned",
    }
    weekly_plan = []
    for item in week_plan_state:
        day_date = date.fromisoformat(item["date"])
        ui_status = _state_to_status[item["state"]]
        is_current_day = day_date == today_local
        weekly_plan.append({
            "day": item["day_name"].capitalize(),
            "day_date": day_date,
            "date": day_date,
            "workout_type": item["workout_type"],
            "session_type": item["session_type"],
            "session": item["session_name"],
            "planned_km": item["planned_distance_km"],
            "display_planned_km": _display_planned_km(item["planned_distance_km"]),
            "actual_km": item["actual_run_km"],
            "done": item["state"] == DONE,
            "status": _state_to_status[item["state"]],
            "state": item["state"],
            "is_today": item["state"] == TODAY or is_current_day,
            "pace_guidance": item.get("pace_guidance", ""),
            "notes": item.get("notes", ""),
            "actual_strength_count": item["actual_strength_count"],
            "actual_walk_km": item["actual_walk_km"],
            "actual_cross_train_km": item["actual_cross_train_km"],
            "alternate_activity_text": "",
            "status_label": "",
        })
        weekly_plan[-1]["alternate_activity_text"] = (
            _format_alternate_activity_text(weekly_plan[-1], is_today=is_current_day) if ui_status == "different_activity" else ""
        )
        weekly_plan[-1]["status_label"] = (
            _different_activity_status_label(weekly_plan[-1], is_today=is_current_day) if ui_status == "different_activity" else ui_status
        )
        persisted_status = _persisted_status_map[ui_status]
        upsert_workout_log(
            user_id=user.id,
            workout_date=day_date,
            workout_type=item["workout_type"],
            session_name=item["session_name"],
            target_distance_km=item["planned_distance_km"] if item["planned_distance_km"] > 0 else None,
            status=persisted_status,
            actual_distance_km=item["actual_run_km"] if item["actual_run_km"] > 0 else None,
            notes=item.get("notes", ""),
            source="engine",
            auto_commit=False,
        )
    commit_all()

    weekly_plan_goal_km = week_metrics["planned_km"]
    weekly_plan_completed_km = week_metrics["actual_km"]
    weekly_plan_remaining_km = week_metrics["remaining_km"]
    weekly_longest_run_km = week_metrics["longest_run_km"]
    weekly_planned_long_run_km = week_metrics["planned_long_run_km"]
    weekly_long_run_goal_met = week_metrics["long_run_goal_met"]
    weekly_quality_goal_met = week_metrics["quality_goal_met"]
    weekly_strength_goal_met = week_metrics["strength_goal_met"]

    for item in weekly_plan:
        item["display_planned_km"] = _display_planned_km(item.get("planned_km") or 0.0)

    current_week_model = _derive_current_week_display_metrics(weekly_plan, canonical_weekly_target_km)
    weekly_plan_goal_km = current_week_model["weekly_target_km"]
    weekly_plan_completed_km = current_week_model["actual_km"]
    weekly_plan_remaining_km = current_week_model["remaining_km"]
    weekly_longest_run_km = current_week_model["longest_run_km"]
    weekly_planned_long_run_km = current_week_model["planned_long_run_km"]
    weekly_long_run_goal_met = current_week_model["long_run_goal_met"]
    weekly_quality_goal_met = current_week_model["quality_goal_met"]
    weekly_quality_session_name = current_week_model["quality_session_name"]
    weekly_quality_session_day = current_week_model["quality_session_day"]
    weekly_strength_goal_met = current_week_model["strength_goal_met"]
    week_actual_km = current_week_model["actual_km"]
    progress_pct = current_week_model["progress_pct"]
    display_weekly_target_km = int(round(float(canonical_weekly_target_km or 0.0)))
    display_weekly_remaining_km = max(0, round(display_weekly_target_km - week_actual_km, 1))
    display_long_run_target_km = max(
        [
            int(item.get("display_planned_km") or 0)
            for item in weekly_plan
            if item.get("workout_type") == "RUN" and item.get("session") == "Long Run"
        ]
        or [0]
    )
    canonical_long_run_progression = _deterministic_long_run_progression(
        intel,
        week_start,
        display_weekly_target_km,
        display_long_run_target_km,
        schedule_prefs=schedule_prefs,
    )
    _long_run_summary = intel.get("long_run") or {}
    recent_long_run_km = round(float(_long_run_summary.get("latest_km") or _long_run_summary.get("longest_km") or 0.0), 1)
    recent_long_run_date = _long_run_summary.get("latest_date") or _long_run_summary.get("longest_date")
    try:
        recent_long_run_date_display = datetime.fromisoformat(recent_long_run_date).strftime("%a %d %b") if recent_long_run_date else ""
    except Exception:
        recent_long_run_date_display = recent_long_run_date or ""
    weekly_plan_completion_pct = (
        min(100, int(round(weekly_plan_completed_km / max(1.0, weekly_plan_goal_km) * 100)))
        if weekly_plan_goal_km > 0 else 0
    )
    week_closed = (
        _now_local_datetime(user_tz) >
        datetime.combine(week_end, datetime.max.time()).replace(tzinfo=_now_local_datetime(user_tz).tzinfo)
    )
    weekly_status = (
        "Goal achieved" if weekly_plan_completed_km >= weekly_plan_goal_km
        else "Goal not achieved" if week_closed
        else "In progress"
    )
    weekly_extra_km = 0.0

    # ?? Weekly signal ?????????????????????????????????????????????????????????
    if progress_pct >= 100:
        weekly_signal       = "Strong consistency ??"
        weekly_signal_color = "green"
    elif progress_pct >= 70:
        weekly_signal       = "On track ?"
        weekly_signal_color = "green"
    elif week_closed and progress_pct < 50:
        weekly_signal       = "Tough week ? fresh start ??"
        weekly_signal_color = "muted"
    elif progress_pct >= 40:
        weekly_signal       = "Building ?"
        weekly_signal_color = "green"
    else:
        weekly_signal       = "Consistency slipping ??"
        weekly_signal_color = "amber"

    # ?? Upcoming long runs ? with formatted display dates ????????????????????
    upcoming_long_runs = _build_upcoming_long_runs(
        weekly_plan,
        canonical_long_run_progression,
        today_local,
        limit=4,
    )
    for _w in upcoming_long_runs:
        try:
            _wd = datetime.strptime(_w["week_date"], "%Y-%m-%d")
            _w["week_date_display"] = f"{_wd.strftime('%a')} {_wd.day} {_wd.strftime('%b')}"
        except Exception:
            _w["week_date_display"] = _w.get("week_date", "")

    _long_run_footer = intel.get("long_run", {}) or {}
    footer_long_run_km = round(float(_long_run_footer.get("latest_km") or _long_run_footer.get("longest_km") or 0.0), 1)
    _lr_date_raw = _long_run_footer.get("latest_date") or _long_run_footer.get("longest_date")
    if _lr_date_raw:
        try:
            _lrd = datetime.strptime(_lr_date_raw[:10], "%Y-%m-%d")
            longest_date_display = f"{_lrd.strftime('%a')} {_lrd.day} {_lrd.strftime('%b')}"
        except Exception:
            longest_date_display = _lr_date_raw
    else:
        longest_date_display = None

    # ?? Today's workout card ? single source is weekly_plan[today] ???????????
    today_plan_state = today_session_from_plan(week_plan_state, today_local)
    today_item = next((w for w in weekly_plan if w["day_date"] == today_local), None)
    tomorrow_item = weekly_plan[today_local.weekday() + 1] if today_local.weekday() < 6 else None

    if tomorrow_item:
        _tmrw_date = today_local + timedelta(days=1)
        _tmrw_session = tomorrow_item["session"]
        _tmrw_km = tomorrow_item.get("planned_km", 0)
        _tmrw_type = tomorrow_item.get("session_type", "")
        _tmrw_km_str = (
            f" | {int(_tmrw_km)}km" if _tmrw_km > 0 and _tmrw_type not in ("strength", "rest", "active_recovery")
            else ""
        )
        tomorrow_display = f"{_tmrw_date.strftime('%a %d %b')} | {_tmrw_session}{_tmrw_km_str}"
    else:
        tomorrow_display = "Rest Day"

    def _fmt_actual_km(km):
        return f"{km:.1f} km" if km else "?"

    def _fmt_target_km(km):
        return f"{_display_planned_km(km)} km" if km else "?"

    _status_label = {
        "completed": "Completed",
        "overdone": "Over target",
        "partial": "Partial",
        "missed": "Missed",
        "skipped": "Skipped",
        "different_activity": "Other activity done",
        "today": "Planned",
        "planned": "Planned",
        "rest": "Rest",
    }
    if today_item and today_item["status"] == "different_activity":
        if today_item.get("workout_type") == "RUN":
            _status_label["different_activity"] = "Run open"
        elif today_item.get("workout_type") == "STRENGTH":
            _status_label["different_activity"] = "Gym open"
    completed_summary = ""
    if today_item and today_item["done"]:
        if today_item.get("session_type") == "strength":
            completed_summary = (
                f"Gym completed - plus {today_item['actual_km']:.1f} km run"
                if (today_item.get("actual_km") or 0.0) > 0
                else "Gym completed"
            )
        else:
            completed_summary = f"{_fmt_actual_km(today_item['actual_km'])} (target {_fmt_target_km(today_item['planned_km'])})"
    today_workout = {
        "date":            today_local.strftime("%A, %d %b"),
        "workout":         today_item["session"] if today_item else (today_plan_state["session_name"] if today_plan_state else "Rest Day"),
        "workout_type":    today_item["workout_type"] if today_item else (today_plan_state["workout_type"] if today_plan_state else "REST"),
        "status":          _status_label.get(today_item["status"], "Planned") if today_item else "Rest",
        "completed":       today_item["done"] if today_item else False,
        "distance_actual": _fmt_actual_km(today_item["actual_km"]) if today_item else "?",
        "distance_target": (
            "Gym | strength session"
            if today_item and today_item.get("session_type") == "strength"
            else _fmt_target_km(today_item["planned_km"]) if today_item else "?"
        ),
        "tomorrow":        tomorrow_display,
        "session":         today_item["session"] if today_item else "Rest Day",
        "planned_km":      today_item["planned_km"] if today_item else 0,
        "actual_km":       today_item["actual_km"] if today_item else 0,
        "pace_guidance":   today_item["pace_guidance"] if today_item else "",
        "notes":           today_item["notes"] if today_item else "",
        "alternate_activity_text": (
            _format_alternate_activity_text(today_item, is_today=True) if today_item and today_item["status"] == "different_activity" else ""
        ),
        "completed_summary": completed_summary,
    }
    current_week_coaching_message = _build_current_week_coaching_message(
        float(display_weekly_target_km),
        week_actual_km,
        weekly_longest_run_km,
        float(display_long_run_target_km),
        weekly_long_run_goal_met,
        weekly_quality_goal_met,
        today_item,
        weekly_quality_session_name,
        recent_long_run_km,
        recent_long_run_date_display,
    )
    canonical_phase_label = _deterministic_phase_label(intel)
    canonical_feasibility = _deterministic_feasibility_fields(
        intel,
        current_week_model,
        display_weekly_target_km=display_weekly_target_km,
        display_long_run_target_km=display_long_run_target_km,
    )
    canonical_feasibility_label_emoji = canonical_feasibility.get("label", "")

    consistency_score = _training_consistency_score(user.id, today_local)

    # ?? Recent Activities (all types) ????????????????????????????????????????
    _TYPE_ICONS = {
        "run": "🏃", "trail run": "🏃", "trail_run": "🏃", "track": "🏃",
        "virtualrun": "🏃", "treadmill": "🏃",
        "strength": "🏋️", "weight_training": "🏋️", "strength_training": "🏋️",
        "crossfit": "🏋️", "core": "🏋️", "workout": "🏋️", "flexibility": "🧘",
        "yoga": "🧘", "pilates": "🧘",
        "walk": "🚶", "hike": "🥾",
        "ride": "🚴", "virtualride": "🚴", "cycling": "🚴",
        "swim": "🏊", "swimming": "🏊",
        "rowing": "🚣", "elliptical": "💪", "stairstepper": "🪜",
        "hiit": "🔥", "aerobics": "🤸",
    }

    def _fmt_pace(moving_time, distance_km):
        if not distance_km or distance_km <= 0 or not moving_time:
            return None
        sec_per_km = moving_time / distance_km
        return f"{int(sec_per_km // 60)}:{int(sec_per_km % 60):02d}"

    def _fmt_dur(moving_time):
        if not moving_time:
            return None
        t = int(moving_time)
        h, m = divmod(t // 60, 60)
        return f"{h}h {m:02d}m" if h else f"{m}m"

    _recent_acts_raw = (
        Activity.query
        .filter(Activity.user_id == user.id)
        .order_by(Activity.date.desc())
        .limit(7)
        .all()
    )
    recent_activities = []
    for _a in _recent_acts_raw:
        _typ  = (_a.activity_type or "unknown").lower()
        _dt   = _a.date.strftime("%b %d") if hasattr(_a.date, "strftime") else str(_a.date)[:10]
        recent_activities.append({
            "date":        _dt,
            "type":        _a.activity_type or "unknown",
            "icon":        _TYPE_ICONS.get(_typ, "?"),
            "distance_km": round(_a.distance_km or 0, 1),
            "show_distance": bool((_a.distance_km or 0) > 0),
            "pace":        _fmt_pace(_a.moving_time, _a.distance_km),
            "duration":    _fmt_dur(_a.moving_time),
            "hr":          int(float(_a.avg_hr)) if _a.avg_hr else None,
            "elevation":   int(round(float(_a.elevation_gain))) if _a.elevation_gain and float(_a.elevation_gain) > 10 else None,
            "is_run":      _typ in _RUN_TYPES,
            "is_strength": _typ in _STRENGTH_TYPES,
            "is_cross":    _typ in _CROSS_TRAINING_TYPES,
            "is_recovery": _typ in _RECOVERY_TYPES,
        })

    runs = recent_runs(user.id, limit=5, user_timezone=user_tz)
    weekly_summary = weekly_training_summary(user.id)

    # ── 80/20 intensity split for this week ───────────────────────────────────
    # Easy run = pace significantly slower than goal marathon pace (athlete is
    # building aerobic base). Hard/quality = at or faster than goal pace.
    # Non-run activities always count as easy (cross-training / recovery).
    # Result: {"easy_pct": 72, "hard_pct": 28, "easy_km": 24.0, "hard_km": 9.3}
    _intensity_split = {"easy_pct": 0, "hard_pct": 0, "easy_km": 0.0, "hard_km": 0.0}
    try:
        _goal_pace_sec = None
        _goal_obj = intel.get("goal") or {}
        _goal_time_str = _goal_obj.get("goal_time", "")
        _goal_dist_km  = float(_goal_obj.get("distance_km") or 42.195)
        if _goal_time_str:
            _parts = _goal_time_str.split(":")
            if len(_parts) == 3:
                _goal_total_sec = int(_parts[0]) * 3600 + int(_parts[1]) * 60 + int(_parts[2])
                _goal_pace_sec  = _goal_total_sec / _goal_dist_km  # sec/km at goal pace

        _week_acts = fetch_activities_between(user.id, week_start, week_end)
        _easy_km = _hard_km = 0.0
        _RUN_ACTIVITY_TYPES = {"run", "trail run", "trail_run", "track", "virtualrun", "treadmill"}
        for _act in _week_acts:
            _dist = float(_act.distance_km or 0.0)
            if _dist <= 0:
                continue
            _atype = (_act.activity_type or "").lower()
            if _atype not in _RUN_ACTIVITY_TYPES:
                _easy_km += _dist
                continue
            # For runs: compare actual pace to goal pace.
            # Easy threshold = goal pace * 1.05 (5% slower than goal = easy)
            _mv = float(_act.moving_time or 0.0)
            _pace = _mv / _dist if _dist > 0 else None
            if _goal_pace_sec and _pace and _pace > _goal_pace_sec * 1.05:
                _easy_km += _dist
            else:
                _hard_km += _dist

        _total_km = _easy_km + _hard_km
        if _total_km > 0:
            _intensity_split = {
                "easy_pct": int(round(_easy_km / _total_km * 100)),
                "hard_pct": int(round(_hard_km / _total_km * 100)),
                "easy_km":  round(_easy_km, 1),
                "hard_km":  round(_hard_km, 1),
            }
    except Exception:
        pass  # Never break the dashboard over a non-critical metric

    # ── Prediction trend sparkline ─────────────────────────────────────────────
    # Fetch up to 10 historical predictions (oldest-first) so the template can
    # render a mini sparkline showing how the marathon projection has evolved.
    # Each row is formatted as {label, seconds} — the JS side converts seconds
    # to a display time string using the same _fmt helper used for other cards.
    _pred_history = fetch_recent_predictions(user.id, limit=10)
    prediction_trend_json = json.dumps([
        {
            "label": row.created_at.strftime("%b %d") if row.created_at else "—",
            "seconds": round(float(row.projection_seconds)),
        }
        for row in _pred_history
        if row.projection_seconds and row.projection_seconds > 0
    ])

    weekly_plan_note = (
        "If you do both gym and a run on a strength day, the gym session counts as completed"
        " and the run counts toward weekly mileage."
        " If you skip the gym and only run, the run still counts toward mileage,"
        " but the strength session stays incomplete."
    )

    # ── Projected race-day TSB ─────────────────────────────────────────────────
    # Uses ATL/CTL exponential decay (assuming taper = minimal training) to
    # estimate the athlete's form on race day.  Ideal race-day TSB: +5 to +15.
    # ATL decay constant = 7 days; CTL decay constant = 42 days.
    _proj_tsb = None
    try:
        _d2r = int((intel.get("goal") or {}).get("days_remaining") or 0)
        _c_atl = float(intel.get("current_atl") or 0.0)
        _c_ctl = float(intel.get("current_ctl") or 0.0)
        if _d2r > 0 and _c_ctl > 0:
            _p_atl = _c_atl * math.exp(-_d2r / 7.0)
            _p_ctl = _c_ctl * math.exp(-_d2r / 42.0)
            _proj_tsb = round(_p_ctl - _p_atl, 1)
    except Exception:
        pass  # Non-critical — never break the dashboard

    return render_template(
        "dashboard.html",
        user=user,
        goal=intel["goal"],
        intel=intel,
        weekly_summary=weekly_summary,
        endurance=intel["endurance"],
        show_today_plan=not today_workout["completed"],
        today_workout=today_workout,
        weekly_plan=weekly_plan,
        consistency_score=consistency_score,
        prediction_confidence_label=intel.get("prediction_confidence", "Building"),
        weekly_plan_goal_km=weekly_plan_goal_km,
        weekly_plan_completed_km=weekly_plan_completed_km,
        weekly_extra_km=weekly_extra_km,
        weekly_plan_remaining_km=weekly_plan_remaining_km,
        weekly_completion_pct=weekly_plan_completion_pct,
        progress_pct=progress_pct,
        week_actual_km=week_actual_km,
        weekly_status=weekly_status,
        weekly_plan_note=weekly_plan_note,
        week_closed=week_closed,
        weekly_longest_run_km=weekly_longest_run_km,
        weekly_planned_long_run_km=weekly_planned_long_run_km,
        weekly_long_run_goal_met=weekly_long_run_goal_met,
        weekly_quality_goal_met=weekly_quality_goal_met,
        weekly_quality_session_name=weekly_quality_session_name,
        weekly_quality_session_day=weekly_quality_session_day,
        weekly_strength_goal_met=weekly_strength_goal_met,
        recent_activities=recent_activities,
        runs=runs,
        sync_info=sync_info,
        today_date=_today_date_label(user_tz),
        long_run=intel["long_run"],
        banner=dq_report["banner"],
        show_banner=dq_report["show_banner"],
        dq=dq_report,
        canonical_phase_label=canonical_phase_label,
        canonical_weekly_target_km=canonical_weekly_target_km,
        display_weekly_target_km=display_weekly_target_km,
        display_weekly_remaining_km=display_weekly_remaining_km,
        display_long_run_target_km=display_long_run_target_km,
        canonical_long_run_km=canonical_long_run_km,
        canonical_coaching_message=canonical_coaching_message,
        current_week_coaching_message=current_week_coaching_message,
        canonical_alerts=canonical_alerts,
        canonical_week_theme=canonical_week_theme,
        canonical_focus_point=canonical_focus_point,
        canonical_long_run_progression=canonical_long_run_progression,
        upcoming_long_runs=upcoming_long_runs,
        future_week_preview=_deterministic_future_week_preview(
            intel,
            week_start,
            display_weekly_target_km,
            display_long_run_target_km,
            limit=3,
            schedule_prefs=schedule_prefs,
        ),
        week_remaining_km=display_weekly_remaining_km,
        recent_long_run_km=recent_long_run_km,
        recent_long_run_date_display=recent_long_run_date_display,
        canonical_feasibility=canonical_feasibility,
        canonical_feasibility_score=canonical_feasibility.get("score", 0),
        canonical_feasibility_color=canonical_feasibility.get("color", "grey"),
        canonical_feasibility_label=canonical_feasibility_label_emoji,
        canonical_honest_assessment=canonical_feasibility.get("text", ""),
        canonical_show_revised_goal=False,
        canonical_revised_goal=canonical_feasibility.get("revised_goal") or {},
        canonical_feasibility_factor_scores={},
        tsb_volume_cap=_coaching_plan.get("validation", {}).get("tsb_volume_cap", 1.0),
        tsb_quality_allowed=not _coaching_plan.get("validation", {}).get("quality_replaced", False),
        canonical_long_run_day=canonical_long_run_day,
        aerobic_pace_display=aerobic_pace_display,
        weekly_ctl_json=json.dumps(weekly_ctl_series),
        longest_date_display=longest_date_display,
        footer_long_run_km=footer_long_run_km,
        show_wall_estimate=show_wall_estimate,
        canonical_wall_adjusted_seconds=canonical_wall_adjusted_seconds,
        canonical_wall_cost_minutes=canonical_wall_cost_minutes,
        canonical_wall_base_display=canonical_wall_base_display,
        canonical_wall_adjusted_display=canonical_wall_adjusted_display,
        canonical_wall_factor=_wall_factor,
        canonical_wall_risk=_wall_risk,
        fm_gap_display=fm_gap_display,
        fm_gap_color=fm_gap_color,
        wall_actions=wall_actions,
        weekly_signal=weekly_signal,
        weekly_signal_color=weekly_signal_color,
        prediction_trend_json=prediction_trend_json,
        intensity_split=_intensity_split,
        target_ctl=62 if float((intel.get("goal") or {}).get("distance_km") or 42.195) >= 40 else 55,
        projected_tsb=_proj_tsb,
    )

# ---------------------------------------------------------------------------
# Coach intro — conversational AI onboarding (between /onboarding and /)
# ---------------------------------------------------------------------------

# Hardcoded fallback messages per step when Claude API is unavailable.
_COACH_FALLBACKS = {
    0: {
        "text": "Welcome! I'm Coach Ike, your AI marathon coach. I have 4 quick questions to personalise your training plan. First — how consistently have you been running recently?",
        "options": ["Just getting started (< 1 month)", "Getting back into it (had a break)", "Training consistently (3+ months)", "Well trained (6+ months solid)"],
        "input_type": "options",
    },
    1: {
        "text": "Good to know. Have you completed this race distance before?",
        "options": ["First time at this distance", "Done it once before", "Done it multiple times"],
        "input_type": "options",
    },
    2: {
        "text": "Got it. Any injuries or niggles I should factor in from the last 6 months?",
        "options": ["Fully healthy — no issues", "Minor issue, mostly recovered", "Managing an ongoing issue"],
        "input_type": "options",
    },
    3: {
        "text": "Last one — what matters most to you about this race?",
        "options": ["Just finish healthy and strong", "Beat my previous time", "Hit my specific time goal", "Qualify for a major race"],
        "input_type": "options",
    },
}


_STEP_KEYS = [
    "consistency_level",
    "race_experience",
    "injury_status",
    "goal_priority",
]


def _get_next_coach_message(current_step, user_answer, collected, goal, user) -> dict:
    """Call Claude to generate the next conversational coaching message.
    Falls back to hardcoded questions if the API is unavailable."""
    dist_km = float(getattr(goal, "race_distance", None) or 42.2)
    if dist_km > 40:
        dist_label = "marathon"
    elif dist_km > 20:
        dist_label = "half marathon"
    elif dist_km > 9:
        dist_label = "10K"
    else:
        dist_label = "5K"

    prompt = (
        f"You are a warm, encouraging elite marathon coach named Coach Ike "
        f"having your first conversation with a new runner.\n\n"
        f"Runner's goal: {goal.goal_time} {dist_label} on {goal.race_date} ({goal.race_name})\n"
        f"Runner's name: {user.name or 'there'}\n\n"
        f"Conversation so far:\n{json.dumps(collected, indent=2)}\n\n"
        f"Current step: {current_step} of 3\n"
        f"Previous answer: \"{user_answer}\"\n\n"
        f"Generate the next conversational message for step {current_step}.\n\n"
        f"STEP DEFINITIONS (only 4 steps total — 0 through 3):\n"
        f"Step 0: Warm welcome using runner's name and their specific goal. Mention this is just 4 quick questions. Ask how consistently they have been running recently.\n"
        f"  Options: [\"Just getting started (< 1 month)\", \"Getting back into it (had a break)\", \"Training consistently (3+ months)\", \"Well trained (6+ months solid)\"]\n"
        f"Step 1: Brief acknowledgment. Ask if they have completed this distance before.\n"
        f"  Options: [\"First time at this distance\", \"Done it once before\", \"Done it multiple times\"]\n"
        f"Step 2: Brief acknowledgment. Ask about injuries or niggles in last 6 months.\n"
        f"  Options: [\"Fully healthy — no issues\", \"Minor issue, mostly recovered\", \"Managing an ongoing issue\"]\n"
        f"Step 3: Brief acknowledgment (if injury mentioned, show empathy). Ask what matters most to them about this race. This is the LAST question — end warmly.\n"
        f"  Options: [\"Just finish healthy and strong\", \"Beat my previous time\", \"Hit my specific time goal\", \"Qualify for a major race\"]\n\n"
        f"RULES:\n"
        f"- Keep each message SHORT — 1-2 sentences max before the question\n"
        f"- Be warm, personal, specific to their goal and race name\n"
        f"- If injury mentioned, acknowledge with care\n"
        f"- Never be generic\n\n"
        f"Respond ONLY with valid JSON (no markdown, no code fences):\n"
        f"{{\"text\": \"your message\", \"options\": [\"opt1\", \"opt2\", ...], \"input_type\": \"options\", \"collected\": {{...updated dict...}}}}"
    )

    api_key = None
    model = "claude-sonnet-4-5"
    try:
        api_key = current_app.config.get("ANTHROPIC_API_KEY")
        model = current_app.config.get("ANTHROPIC_MODEL", "claude-sonnet-4-5")
    except RuntimeError:
        pass

    if api_key:
        for _attempt in range(2):
            try:
                resp = http_requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": model,
                        "max_tokens": 500,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    raw = resp.json()["content"][0]["text"].strip()
                    raw = raw.replace("```json", "").replace("```", "").strip()
                    parsed = json.loads(raw)
                    # Ensure required keys present
                    if "text" in parsed and "options" in parsed:
                        return parsed
            except Exception:
                pass

    # Fallback: return hardcoded question for this step
    fallback = _COACH_FALLBACKS.get(current_step) or _COACH_FALLBACKS.get(len(_COACH_FALLBACKS) - 1, {})
    return {**fallback, "collected": collected}


def _save_runner_profile(user_id: int, collected: dict):
    """Persist collected onboarding answers into RunnerProfile."""
    consistency_map = {
        "Just getting started (< 1 month)": "just_starting",
        "Getting back into it (had a break)": "getting_back",
        "Training consistently (3+ months)": "consistent",
        "Well trained (6+ months solid)": "well_trained",
    }
    experience_map = {
        "First time at this distance": "first_time",
        "Done it once before": "once",
        "Done it multiple times": "multiple",
    }
    injury_map = {
        "Fully healthy — no issues": "healthy",
        "Minor issue, mostly recovered": "minor",
        "Managing an ongoing issue": "ongoing",
    }
    days_map = {"3 days": 3, "4 days": 4, "5 days": 5, "6 days": 6}
    long_run_map = {
        "Saturday": "saturday",
        "Sunday": "sunday",
        "Flexible — no preference": "flexible",
    }
    strength_map = {
        "No strength training": 0,
        "1 session per week": 1,
        "2 sessions per week": 2,
    }
    run_time_map = {
        "Early morning (before 7am)": "early_morning",
        "Morning (7-10am)": "morning",
        "Evening (after 5pm)": "evening",
        "Flexible — it varies": "flexible",
    }
    priority_map = {
        "Just finish healthy and strong": "finish_healthy",
        "Beat my previous time": "beat_previous",
        "Hit my specific time goal": "hit_time",
        "Qualify for a major race": "qualify",
    }

    profile = RunnerProfile.query.filter_by(user_id=user_id).first()
    if not profile:
        profile = RunnerProfile(user_id=user_id)

    profile.consistency_level      = consistency_map.get(collected.get("consistency_level"), "consistent")
    profile.race_experience        = experience_map.get(collected.get("race_experience"), "once")
    profile.injury_status          = injury_map.get(collected.get("injury_status"), "healthy")
    profile.training_days_per_week = days_map.get(collected.get("training_days_per_week"), 5)
    profile.long_run_day           = long_run_map.get(collected.get("long_run_day"), "sunday")
    profile.strength_days_per_week = strength_map.get(collected.get("strength_days_per_week"), 2)
    profile.preferred_run_time     = run_time_map.get(collected.get("preferred_run_time"), "flexible")
    profile.goal_priority          = priority_map.get(collected.get("goal_priority"), "hit_time")
    profile.onboarding_completed   = True
    profile.completed_at           = _utcnow_naive()

    db.session.add(profile)
    db.session.commit()


@web.route("/coach-intro", methods=["GET"])
@login_required
def coach_intro():
    """Conversational AI onboarding — runs once, between goal setup and dashboard."""
    user = _current_user()

    # Already completed → skip to dashboard
    profile = RunnerProfile.query.filter_by(user_id=user.id).first()
    if profile and profile.onboarding_completed:
        return redirect(url_for("web.dashboard"))

    goal = Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first()
    if not goal:
        return redirect(url_for("web.onboarding"))

    four_weeks_ago = _utcnow_naive() - timedelta(weeks=4)
    activity_count = Activity.query.filter(
        Activity.user_id == user.id,
        Activity.date >= four_weeks_ago,
    ).count()

    from datetime import date as _date
    try:
        race_date = goal.race_date if isinstance(goal.race_date, _date) else _date.fromisoformat(str(goal.race_date))
        days_to_race = (race_date - _date.today()).days
    except Exception:
        days_to_race = None

    return render_template(
        "coach_intro.html",
        user=user,
        goal=goal,
        days_to_race=days_to_race,
        has_strava_data=activity_count > 0,
        strava_activity_count=activity_count,
    )


@web.route("/api/coach-intro/message", methods=["POST"])
@csrf.exempt  # JSON endpoint called by JS fetch — session auth enforces identity
@login_required
def coach_intro_message():
    """Handle one turn of the conversational onboarding chat."""
    user = _current_user()
    data = request.get_json(silent=True) or {}

    conversation   = data.get("conversation", [])
    user_answer    = data.get("user_answer", "")
    current_step   = int(data.get("current_step", 0))
    collected      = dict(data.get("collected", {}))

    goal = Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first()
    if not goal:
        return jsonify({"error": "no_goal"}), 400

    # Collect the previous step's answer
    if current_step > 0 and user_answer and current_step - 1 < len(_STEP_KEYS):
        collected[_STEP_KEYS[current_step - 1]] = user_answer

    # All 4 steps answered → save and redirect
    if current_step >= 4:
        _save_runner_profile(user.id, collected)
        return jsonify({"done": True, "redirect": url_for("web.dashboard")})

    msg = _get_next_coach_message(current_step, user_answer, collected, goal, user)

    return jsonify({
        "done": False,
        "message": msg.get("text", ""),
        "options": msg.get("options", []),
        "input_type": msg.get("input_type", "options"),
        "next_step": current_step + 1,
        "collected": msg.get("collected", collected),
    })


@web.route("/onboarding", methods=["GET", "POST"])
@login_required
def onboarding():
    user = _current_user()
    error = request.args.get("error")

    if request.method == "POST":
        runner_name = request.form.get("runner_name", "").strip()
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "moderate").strip()
        personal_best = request.form.get("current_pb", "").strip()
        pb_5k  = request.form.get("pb_5k",  "").strip()
        pb_10k = request.form.get("pb_10k", "").strip()
        pb_hm  = request.form.get("pb_hm",  "").strip()

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        if runner_name and race_name and race_date and goal_time and race_distance > 0:
            try:
                update_user_name(user.id, runner_name)
                save_goal(
                    user_id=user.id,
                    race_name=race_name,
                    race_distance=race_distance,
                    goal_time=goal_time,
                    race_date=race_date,
                    elevation_type=elevation_type,
                    personal_best=personal_best,
                    pb_5k=pb_5k,
                    pb_10k=pb_10k,
                    pb_hm=pb_hm,
                )
                return redirect(url_for("web.coach_intro"))
            except Exception:
                error = "Unable to save goal right now. Please try again."

    return render_template("onboarding.html", user=user, goal=get_goal(user.id), error=error)


@web.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    user = _current_user()
    goal = get_goal(user.id)
    profile = RunnerProfile.query.filter_by(user_id=user.id).first()
    error = request.args.get("error")

    if request.method == "POST":
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "moderate").strip()
        personal_best = request.form.get("current_pb", "").strip()
        pb_5k  = request.form.get("pb_5k",  "").strip()
        pb_10k = request.form.get("pb_10k", "").strip()
        pb_hm  = request.form.get("pb_hm",  "").strip()
        training_days_per_week = request.form.get("training_days_per_week", "").strip()
        long_run_day = request.form.get("long_run_day", "").strip().lower()
        strength_days_per_week = request.form.get("strength_days_per_week", "").strip()
        preferred_run_time = request.form.get("preferred_run_time", "").strip().lower()

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        try:
            training_days_per_week = int(training_days_per_week or 5)
        except ValueError:
            training_days_per_week = 5
        try:
            strength_days_per_week = int(strength_days_per_week or 2)
        except ValueError:
            strength_days_per_week = 2

        training_days_per_week = max(3, min(6, training_days_per_week))
        strength_days_per_week = max(0, min(2, strength_days_per_week))
        if long_run_day not in {"saturday", "sunday", "flexible"}:
            long_run_day = "sunday"
        if preferred_run_time not in {"early_morning", "morning", "evening", "flexible"}:
            preferred_run_time = "flexible"

        if race_name and race_date and goal_time and race_distance > 0:
            try:
                save_goal(
                    user_id=user.id,
                    race_name=race_name,
                    race_distance=race_distance,
                    goal_time=goal_time,
                    race_date=race_date,
                    elevation_type=elevation_type,
                    personal_best=personal_best,
                    pb_5k=pb_5k,
                    pb_10k=pb_10k,
                    pb_hm=pb_hm,
                )
                if not profile:
                    profile = RunnerProfile(user_id=user.id)
                profile.training_days_per_week = training_days_per_week
                profile.long_run_day = long_run_day
                profile.strength_days_per_week = strength_days_per_week
                profile.preferred_run_time = preferred_run_time
                db.session.add(profile)
                db.session.commit()
                return redirect(url_for("web.dashboard"))
            except Exception:
                db.session.rollback()
                error = "Unable to update settings right now. Please try again."

    return render_template("settings.html", user=user, goal=goal, profile=profile, error=error)


@web.route("/connect/strava")
@login_required
def strava_login():
    user = _current_user()
    state = generate_oauth_state()
    session["strava_state"] = state
    session["oauth_user_id"] = user.id
    try:
        return redirect(get_authorize_url(state))
    except Exception:
        current_app.logger.exception("Failed to start Strava OAuth")
        return redirect(url_for("web.onboarding", error="Strava OAuth is not configured. Set CLIENT_ID, CLIENT_SECRET and STRAVA_REDIRECT_URI in Render."))


@web.route("/debug/weekly-target")
@login_required
def debug_weekly_target():
    """Temporary debug endpoint — shows daily plan km and weekly target calculation."""
    from ppi.models import CoachingPlan
    import json as _json
    user = _current_user()
    cp = CoachingPlan.query.filter_by(user_id=user.id).order_by(CoachingPlan.id.desc()).first()
    if not cp:
        return jsonify({"error": "no coaching plan found"})
    plan = _json.loads(cp.plan_json)
    this_week = plan.get("this_week", {})
    daily = this_week.get("daily_plan", {})
    _WEEK_DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    rows = []
    total = 0.0
    for day in _WEEK_DAYS:
        s = daily.get(day, {})
        km = float(s.get("km", 0))
        total += km
        rows.append({"day": day, "type": s.get("type", "—"), "km": km, "session": s.get("session", "")})
    running_rows = [r for r in rows if r["type"] not in ("strength", "rest") and r["km"] > 0]
    running_sum = round(sum(r["km"] for r in running_rows), 1)
    return jsonify({
        "daily_plan": rows,
        "ai_weekly_target_km": this_week.get("weekly_target_km"),
        "calculated_sum_all_km": round(total, 2),
        "calculated_sum_running_km": running_sum,
        "canonical_weekly_target_km": running_sum,
        "long_run_km": this_week.get("long_run", {}).get("km"),
    })


@web.route("/auth/strava/callback")
@limiter.limit("10 per hour")
def strava_callback():
    expected = session.get("strava_state")
    received = request.args.get("state")
    if not expected or expected != received:
        return redirect(url_for("web.login"))

    user_id = session.get("oauth_user_id")
    code = request.args.get("code")
    if not user_id or not code:
        return redirect(url_for("web.login"))

    try:
        payload = exchange_code_for_token(code)
        link_oauth_identity(user_id, payload)
    except Exception:
        current_app.logger.exception("Strava OAuth callback failed")
        return redirect(url_for("web.onboarding", error="Strava authorization failed. Check CLIENT_ID/CLIENT_SECRET and callback URL settings."))

    session["user_id"] = user_id
    session.pop("strava_state", None)
    session.pop("oauth_user_id", None)

    if not get_goal(user_id):
        return redirect(url_for("web.onboarding"))
    return redirect(url_for("web.dashboard"))

