import json
import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from functools import wraps
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests as http_requests
from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .repositories import (
    commit_all,
    consume_password_reset,
    create_password_reset,
    create_user,
    delete_workout_log,
    fetch_activities_between,
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
    build_weekly_plan_template,
    classify_quality_completion as service_classify_quality_completion,
    classify_run_completion as service_classify_run_completion,
    goal_marathon_pace as service_goal_marathon_pace,
    plan_meta_for_session as service_plan_meta_for_session,
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
from .services.prediction_engine import vdot_from_race, vdot_to_race_time_seconds
from .services.strava_service import sync_strava_data
from .services.data_quality import DataQualityReport
from .models import Activity, Goal, RunnerProfile
from .extensions import db

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
        tz = timezone.utc
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


def _build_weekly_plan(user_id, today_local, user_timezone, weekly_goal, long_run, week_start=None):
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
def dashboard():
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

    intel = performance_intelligence(user.id, user_timezone=user_tz)

    if not intel or not intel.get("goal"):
        return redirect(url_for("web.onboarding"))

    weekly_summary = weekly_training_summary(user.id)
    weekly_goal = intel["weekly"]
    runs = recent_runs(user.id, limit=20, user_timezone=user_tz)
    today_local = _today_local_date(user_tz)
    week_start, week_end = _week_bounds(today_local)
    weekly_plan = _build_weekly_plan(user.id, today_local, user_tz, weekly_goal, intel["long_run"], week_start=week_start)
    next_week_plan = []
    next_week_start = week_start + timedelta(days=7)
    next_week_plan = _build_weekly_plan(user.id, today_local, user_tz, weekly_goal, intel["long_run"], week_start=next_week_start)
    next_upcoming_run = _next_upcoming_run_from_plan(today_local, weekly_plan, next_week_plan)
    today_workout = _build_today_workout(today_local, runs, weekly_plan, next_upcoming_run)
    ai_coaching = generate_coaching_output(intel, weekly_plan)
    ai_summary = ai_coaching["coaching_summary"]
    key_session = _pick_key_session(today_local, weekly_plan) if weekly_plan else None
    key_session_importance = key_session.get("importance", "Low") if key_session else "Low"
    planned_runs = [item for item in weekly_plan if item["workout_type"] == "RUN"]
    mileage_plan_runs = [item for item in planned_runs if item["session"] != "Race Day"]
    race_day_item = next((item for item in planned_runs if item["session"] == "Race Day"), None)
    completed_runs = [item for item in mileage_plan_runs if item["status"] == "completed"]
    weekly_plan_goal_km = round(sum(float(item.get("planned_km") or 0.0) for item in mileage_plan_runs), 1)
    weekly_plan_completed_km = round(sum(float(item.get("actual_km") or 0.0) for item in mileage_plan_runs), 1)
    weekly_extra_km = round(max(0.0, float(weekly_goal["completed_km"]) - weekly_plan_completed_km), 1)
    weekly_plan_remaining_km = round(max(0.0, weekly_plan_goal_km - min(weekly_plan_completed_km, weekly_plan_goal_km)), 1)
    weekly_plan_completion_pct = min(100, int(round((weekly_plan_completed_km / max(1.0, weekly_plan_goal_km)) * 100))) if weekly_plan_goal_km > 0 else 0
    race_week_distance_km = round(float(race_day_item.get("planned_km") or 0.0), 1) if race_day_item else 0.0
    race_week_completed_km = round(float(race_day_item.get("actual_km") or 0.0), 1) if race_day_item and race_day_item.get("actual_km") else 0.0
    consistency_score = _training_consistency_score(user.id, today_local)
    week_closed = _now_local_datetime(user_tz) > datetime.combine(week_end, datetime.max.time()).replace(tzinfo=_now_local_datetime(user_tz).tzinfo)
    weekly_completion_pct = weekly_plan_completion_pct
    weekly_status = "Goal achieved" if weekly_plan_completed_km >= weekly_plan_goal_km else "Goal not achieved" if week_closed else "In progress"
    if key_session is None and next_week_plan:
        key_session = _pick_key_session(week_start + timedelta(days=7), next_week_plan)
        key_session_importance = "High" if key_session and key_session["session"] == "Long Run" else "Medium" if key_session and key_session["session"] == "Tempo Run" else "Low"
    weekly_plan_note = (
        "If you do both gym and a run on a strength day, the gym session counts as completed and the run counts toward weekly mileage."
        " If you skip the gym and only run, the run still counts toward mileage, but the strength session stays incomplete."
    )

    return render_template(
        "dashboard.html",
        user=user,
        goal=intel["goal"],
        intel=intel,
        weekly_goal=weekly_goal,
        weekly_summary=weekly_summary,
        endurance=intel["endurance"],
        show_today_plan=not today_workout["completed"],
        today_workout=today_workout,
        weekly_plan=weekly_plan,
        key_session=key_session or {"day": "--", "planned": "--", "session": "No session"},
        key_session_importance=key_session_importance,
        consistency_score=consistency_score,
        prediction_confidence_label=intel.get("prediction_confidence", "Building"),
        weekly_plan_goal_km=weekly_plan_goal_km,
        weekly_plan_completed_km=weekly_plan_completed_km,
        weekly_extra_km=weekly_extra_km,
        weekly_plan_remaining_km=weekly_plan_remaining_km,
        weekly_completion_pct=weekly_completion_pct,
        weekly_status=weekly_status,
        weekly_plan_note=weekly_plan_note,
        week_closed=week_closed,
        race_week_distance_km=race_week_distance_km,
        race_week_completed_km=race_week_completed_km,
        ai_summary=ai_summary,
        ai_coaching=ai_coaching,
        runs=runs[:5],
        sync_info=sync_info,
        today_date=_today_date_label(user_tz),
        long_run=intel["long_run"],
        banner=dq_report["banner"],
        show_banner=dq_report["show_banner"],
        dq=dq_report,
    )


# ---------------------------------------------------------------------------
# Coach intro — conversational AI onboarding (between /onboarding and /)
# ---------------------------------------------------------------------------

# Hardcoded fallback messages per step when Claude API is unavailable.
_COACH_FALLBACKS = {
    0: {
        "text": "Welcome! I'm Coach Ike, your AI marathon coach. I have a few quick questions to personalise your training plan. First — how consistently have you been running recently?",
        "options": ["Just getting started (< 1 month)", "Getting back into it (had a break)", "Training consistently (3+ months)", "Well trained (6+ months solid)"],
        "input_type": "options",
    },
    1: {
        "text": "Great — good to know where you're starting from. Have you completed this race distance before?",
        "options": ["First time at this distance", "Done it once before", "Done it multiple times"],
        "input_type": "options",
    },
    2: {
        "text": "Understood. Any injuries or niggles I should know about in the last 6 months?",
        "options": ["Fully healthy — no issues", "Minor issue, mostly recovered", "Managing an ongoing issue"],
        "input_type": "options",
    },
    3: {
        "text": "Got it. How many days per week can you realistically commit to training?",
        "options": ["3 days", "4 days", "5 days", "6 days"],
        "input_type": "options",
    },
    4: {
        "text": "Perfect. Which day works best for your weekly long run?",
        "options": ["Saturday", "Sunday", "Flexible — no preference"],
        "input_type": "options",
    },
    5: {
        "text": "Good to know. How many strength or gym sessions per week do you usually do?",
        "options": ["No strength training", "1 session per week", "2 sessions per week"],
        "input_type": "options",
    },
    6: {
        "text": "Almost done! When do you usually run?",
        "options": ["Early morning (before 7am)", "Morning (7-10am)", "Evening (after 5pm)", "Flexible — it varies"],
        "input_type": "options",
    },
    7: {
        "text": "Last question — what matters most to you about this race?",
        "options": ["Just finish healthy and strong", "Beat my previous time", "Hit my specific time goal", "Qualify for a major race"],
        "input_type": "options",
    },
}


_STEP_KEYS = [
    "consistency_level",
    "race_experience",
    "injury_status",
    "training_days_per_week",
    "long_run_day",
    "strength_days_per_week",
    "preferred_run_time",
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
        f"Current step: {current_step} of 7\n"
        f"Previous answer: \"{user_answer}\"\n\n"
        f"Generate the next conversational message for step {current_step}.\n\n"
        f"STEP DEFINITIONS:\n"
        f"Step 0: Warm welcome using runner's name and their specific goal. Ask how consistently they have been running recently.\n"
        f"  Options: [\"Just getting started (< 1 month)\", \"Getting back into it (had a break)\", \"Training consistently (3+ months)\", \"Well trained (6+ months solid)\"]\n"
        f"Step 1: Brief acknowledgment. Ask if they have completed this distance before.\n"
        f"  Options: [\"First time at this distance\", \"Done it once before\", \"Done it multiple times\"]\n"
        f"Step 2: Brief acknowledgment. Ask about injuries in last 6 months.\n"
        f"  Options: [\"Fully healthy — no issues\", \"Minor issue, mostly recovered\", \"Managing an ongoing issue\"]\n"
        f"Step 3: Brief acknowledgment (if injury mentioned, show empathy). Ask training days per week.\n"
        f"  Options: [\"3 days\", \"4 days\", \"5 days\", \"6 days\"]\n"
        f"Step 4: Brief acknowledgment. Ask long run day preference.\n"
        f"  Options: [\"Saturday\", \"Sunday\", \"Flexible — no preference\"]\n"
        f"Step 5: Brief acknowledgment. Ask strength training sessions per week.\n"
        f"  Options: [\"No strength training\", \"1 session per week\", \"2 sessions per week\"]\n"
        f"Step 6: Brief acknowledgment. Ask when they usually run.\n"
        f"  Options: [\"Early morning (before 7am)\", \"Morning (7-10am)\", \"Evening (after 5pm)\", \"Flexible — it varies\"]\n"
        f"Step 7: Brief acknowledgment. Ask what matters most to them. After options, add a warm closing:\n"
        f"  Options: [\"Just finish healthy and strong\", \"Beat my previous time\", \"Hit my specific time goal\", \"Qualify for a major race\"]\n\n"
        f"RULES:\n"
        f"- Keep each message SHORT — 2-3 sentences max before the question\n"
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
    fallback = _COACH_FALLBACKS.get(current_step, _COACH_FALLBACKS[7])
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

    # All 8 steps answered → save and redirect
    if current_step >= 8:
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

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

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
                return redirect(url_for("web.dashboard"))
            except Exception:
                error = "Unable to update settings right now. Please try again."

    return render_template("settings.html", user=user, goal=goal, error=error)


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


@web.route("/auth/strava/callback")
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

