import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from functools import wraps
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .repositories import (
    commit_all,
    consume_password_reset,
    create_password_reset,
    create_user,
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
from .services.strava_oauth_service import (
    exchange_code_for_token,
    generate_oauth_state,
    get_authorize_url,
    link_oauth_identity,
)
from .services.strava_service import sync_strava_data

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
    weekly_target = max(18.0, float(weekly_goal.get("weekly_goal_km", 18.0)))
    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    longest_km = float(long_run.get("longest_km") or 0.0)
    next_milestone = float(long_run.get("next_milestone_km") or max(22.0, min(32.0, longest_km + 2.0)))
    if rebuild_mode:
        long_target = max(14.0, min(22.0, weekly_target * 0.30))
        tempo_target = max(6.0, min(10.0, round(weekly_target * 0.16, 1)))
        aerobic_target = max(6.0, min(10.0, round(weekly_target * 0.15, 1)))
        easy_one = max(5.0, min(8.0, round(weekly_target * 0.12, 1)))
    elif phase == "recovery":
        long_target = max(14.0, min(22.0, weekly_target * 0.28))
        tempo_target = max(5.0, min(8.0, round(weekly_target * 0.12, 1)))
        aerobic_target = max(6.0, min(10.0, round(weekly_target * 0.14, 1)))
        easy_one = max(5.0, min(8.0, round(weekly_target * 0.12, 1)))
    elif phase == "taper":
        long_target = max(12.0, min(24.0, weekly_target * 0.30))
        tempo_target = max(6.0, min(10.0, round(weekly_target * 0.15, 1)))
        aerobic_target = max(6.0, min(10.0, round(weekly_target * 0.14, 1)))
        easy_one = max(5.0, min(8.0, round(weekly_target * 0.12, 1)))
    elif phase == "base":
        long_target = max(16.0, min(28.0, min(next_milestone, weekly_target * 0.34)))
        tempo_target = max(6.0, min(12.0, round(weekly_target * 0.16, 1)))
        aerobic_target = max(8.0, min(14.0, round(weekly_target * 0.18, 1)))
        easy_one = max(6.0, min(10.0, round(weekly_target * 0.14, 1)))
    else:
        long_target = max(18.0, min(32.0, min(next_milestone, weekly_target * 0.38)))
        tempo_target = max(8.0, min(16.0, round(weekly_target * 0.20, 1)))
        aerobic_target = max(8.0, min(16.0, round(weekly_target * 0.17, 1)))
        easy_one = max(6.0, min(12.0, round(weekly_target * 0.13, 1)))
    remaining = max(6.0, weekly_target - (long_target + tempo_target + aerobic_target + easy_one))
    easy_two = max(6.0, min(14.0, round(remaining, 1)))
    return {
        0: {"workout_type": "RUN", "session": "Easy Run", "target_km": easy_one, "intensity": "easy", "importance": "Low", "purpose": "Absorb prior load and keep volume consistent."},
        1: {"workout_type": "RUN", "session": "Aerobic Run", "target_km": aerobic_target, "intensity": "aerobic", "importance": "Medium", "purpose": "Build aerobic endurance and support weekly mileage."},
        2: {"workout_type": "STRENGTH", "session": "Strength", "target_km": None, "intensity": "strength", "importance": "Medium", "purpose": "Maintain durability and injury resistance."},
        3: {"workout_type": "RUN", "session": "Tempo Run", "target_km": tempo_target, "intensity": "tempo", "importance": "High", "purpose": "Improve marathon-specific strength and threshold control."},
        4: {"workout_type": "STRENGTH", "session": "Strength", "target_km": None, "intensity": "strength", "importance": "Medium", "purpose": "Support stability and reduce injury risk."},
        5: {"workout_type": "RUN", "session": "Easy Run", "target_km": easy_two, "intensity": "easy", "importance": "Low", "purpose": "Add controlled mileage without excess fatigue."},
        6: {"workout_type": "RUN", "session": "Long Run", "target_km": long_target, "intensity": "long_run", "importance": "High", "purpose": "Build marathon endurance and fueling durability."},
    }


def _classify_run_completion(actual_km, target_km):
    if not target_km or target_km <= 0:
        return "completed", 100
    if actual_km is None or actual_km <= 0:
        return "missed", 0

    completion_pct = int(round((actual_km / target_km) * 100))
    if actual_km >= 0.9 * target_km:
        return "completed", completion_pct
    if actual_km >= 0.5 * target_km:
        return "partial", completion_pct
    return "missed", completion_pct


def _status_label(status):
    mapping = {
        "completed": "Completed",
        "partial": "Partial",
        "missed": "Missed",
        "skipped": "Missed",
        "planned": "Planned",
        "overperformed": "Completed",
    }
    return mapping.get(status, status.title())


def _priority_rank(item):
    order = {"Long Run": 0, "Tempo Run": 1, "Aerobic Run": 2, "Easy Run": 3, "Recovery Run": 4, "Strength": 5}
    return order.get(item.get("session"), 99)


def _plan_meta_for_session(session_name):
    catalog = {
        "Long Run": {"intensity": "long_run", "importance": "High", "purpose": "Build marathon endurance and fueling durability."},
        "Tempo Run": {"intensity": "tempo", "importance": "High", "purpose": "Improve marathon-specific strength and threshold control."},
        "Aerobic Run": {"intensity": "aerobic", "importance": "Medium", "purpose": "Build aerobic endurance and support weekly mileage."},
        "Easy Run": {"intensity": "easy", "importance": "Low", "purpose": "Absorb prior load and keep volume consistent."},
        "Recovery Run": {"intensity": "recovery", "importance": "Low", "purpose": "Reduce fatigue and keep the week moving without strain."},
        "Strength": {"intensity": "strength", "importance": "Medium", "purpose": "Maintain durability and injury resistance."},
    }
    return catalog.get(session_name, {"intensity": "easy", "importance": "Low", "purpose": "Support the weekly training cycle."})


def _fatigue_score(plan_items, weekly_goal_km, today_local):
    score = 0
    recent_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"] and item["date"] < today_local]
    last_two_days_km = sum(item["actual_km"] or 0.0 for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"] and 0 <= (today_local - item["date"]).days <= 2)
    missed_key_sessions = [
        item for item in plan_items
        if item["date"] < today_local and item["workout_type"] == "RUN" and item["session"] in {"Tempo Run", "Long Run"} and item["status"] in {"missed", "partial"}
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
    weekly_goal_km = float(weekly_goal.get("weekly_goal_km") or 0.0)
    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    max_safe_run = float(weekly_goal.get("max_safe_run") or max(10.0, weekly_goal_km * 0.35))
    long_run_failed_recent = bool(weekly_goal.get("long_run_failed_recent"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    moderate_fatigue = bool(weekly_goal.get("moderate_fatigue"))

    missed_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and item["status"] == "missed"]
    partial_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and item["status"] == "partial"]
    overperformed_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and (item["completion_pct"] or 0) >= 115]
    completed_run_km = sum(item["actual_km"] or 0.0 for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"])
    fatigue_score = _fatigue_score(plan_items, weekly_goal_km, today_local)

    future_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] >= today_local and item["status"] == "planned"]
    if not future_runs:
        return plan_items

    # Rebuild mode strips quality and caps the long run.
    if rebuild_mode:
        for item in future_runs:
            if item["session"] == "Tempo Run":
                item["session"] = "Aerobic Run"
                item["adaptive_note"] = "Quality reduced while rebuilding consistency after a gap."
            if item["session"] == "Long Run":
                item["planned_km"] = round(min(item["planned_km"] or 0.0, max_safe_run, 18.0), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Long run capped while rebuilding durability."
            meta = _plan_meta_for_session(item["session"])
            item.update(meta)

    if phase == "recovery":
        for item in future_runs:
            if item["session"] == "Tempo Run":
                item["session"] = "Aerobic Run"
                item["planned_km"] = round(max(6.0, (item["planned_km"] or 0.0) * 0.8), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery week reduces workout intensity while preserving consistency."
                item.update(_plan_meta_for_session(item["session"]))
            elif item["session"] == "Long Run":
                item["planned_km"] = round(min(max_safe_run, max(14.0, (item["planned_km"] or 0.0) * 0.85)), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery week trims long-run stress."

    if len(missed_runs) + len(partial_runs) >= 2:
        for item in future_runs:
            if item["session"] == "Tempo Run":
                item["session"] = "Aerobic Run"
                item["planned_km"] = round(max(6.0, (item["planned_km"] or 0.0) * 0.85), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Quality session reduced after missed work earlier in the week."
                item.update(_plan_meta_for_session(item["session"]))
                break

    if high_fatigue or fatigue_score >= 2:
        for item in future_runs:
            if item["session"] in {"Easy Run", "Aerobic Run"}:
                item["session"] = "Recovery Run"
                item["planned_km"] = round(max(4.0, (item["planned_km"] or 0.0) * 0.75), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery inserted to control fatigue."
                item.update(_plan_meta_for_session(item["session"]))
                break
        for item in future_runs:
            if item["session"] == "Long Run":
                item["planned_km"] = round(min(max_safe_run, max(12.0, (item["planned_km"] or 0.0) * 0.9)), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                if not item.get("adaptive_note"):
                    item["adaptive_note"] = "Long run trimmed slightly to keep fatigue under control."
    elif moderate_fatigue:
        for item in future_runs:
            if item["session"] == "Tempo Run":
                item["planned_km"] = round(max(6.0, (item["planned_km"] or 0.0) * 0.9), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Tempo volume trimmed to keep fatigue stable."
                break

    if long_run_failed_recent:
        for item in future_runs:
            if item["session"] == "Long Run":
                item["planned_km"] = round(min(max_safe_run, max(16.0, (item["planned_km"] or 0.0) * 0.92)), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Long run repeated at a safer step after the last incomplete attempt."
                break

    if overperformed_runs and phase != "taper":
        next_easy = next((item for item in future_runs if item["session"] in {"Easy Run", "Aerobic Run", "Recovery Run"}), None)
        if next_easy:
            next_easy["session"] = "Recovery Run"
            next_easy["planned_km"] = round(max(4.0, (next_easy["planned_km"] or 0.0) * 0.85), 1)
            next_easy["planned"] = f"{int(round(next_easy['planned_km']))} km"
            next_easy["adaptive_note"] = "Recovery added after a bigger-than-planned run."
            next_easy.update(_plan_meta_for_session(next_easy["session"]))

    future_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] >= today_local and item["status"] == "planned"]
    target_remaining = max(0.0, weekly_goal_km - completed_run_km)
    future_total = sum(item["planned_km"] or 0.0 for item in future_runs)
    if future_runs and future_total > 0:
        scale = min(1.15, max(0.8, target_remaining / future_total if target_remaining > 0 else 0.8))
        if phase == "taper":
            scale = min(scale, 1.0)
        for item in future_runs:
            base = float(item["planned_km"] or 0.0)
            if item["session"] == "Long Run":
                long_floor = 12.0 if phase in {"taper", "rebuild", "recovery"} else max(14.0, base * 0.85)
                adjusted = max(long_floor, min(max_safe_run, base * min(scale, 1.0)))
            elif item["session"] == "Recovery Run":
                adjusted = base
            else:
                adjusted = base * scale
            if adjusted > 0:
                item["planned_km"] = round(max(4.0, adjusted), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"

    for item in plan_items:
        item.setdefault("adaptive_note", None)
        item.update(_plan_meta_for_session(item["session"]))
    return plan_items


def _build_weekly_plan(user_id, today_local, user_timezone, weekly_goal, long_run, week_start=None):
    week_start = week_start or _week_bounds(today_local)[0]
    week_end = week_start + timedelta(days=6)
    template = _weekly_plan_template(weekly_goal, long_run)

    existing = {w.workout_date: w for w in fetch_workout_logs(user_id, week_start, week_end)}
    for offset in range(7):
        day_date = week_start + timedelta(days=offset)
        if day_date in existing:
            continue
        plan = template[offset]
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
    for log in logs:
        acts = by_day.get(log.workout_date, [])
        run_acts = [
            a for a in acts
            if (a.activity_type or "").lower() in {"run", "trailrun"} and not a.is_race
        ]
        if log.session_name == "Long Run":
            matched_run = max(run_acts, key=lambda a: a.distance_km, default=None)
            run_km = round(float(matched_run.distance_km), 1) if matched_run else 0.0
        elif log.session_name == "Tempo Run":
            matched_run = min(
                run_acts,
                key=lambda a: ((a.moving_time / max(a.distance_km, 0.1)), -a.distance_km),
                default=None,
            )
            run_km = round(float(matched_run.distance_km), 1) if matched_run else 0.0
        else:
            run_km = round(sum(a.distance_km for a in run_acts), 1)
        strength_done = any((a.activity_type or "").lower() in {"strength", "yoga"} for a in acts)

        new_status = log.status
        new_actual = log.actual_distance_km
        if log.workout_type == "RUN":
            target = float(log.target_distance_km or 0.0)
            if run_km > 0:
                new_status, _ = _classify_run_completion(run_km, target)
                new_actual = run_km
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

        if new_status != log.status or new_actual != log.actual_distance_km:
            upsert_workout_log(
                user_id=user_id,
                workout_date=log.workout_date,
                workout_type=log.workout_type,
                session_name=log.session_name,
                target_distance_km=log.target_distance_km,
                status=new_status,
                actual_distance_km=new_actual,
                notes=log.notes,
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
        if log.workout_type == "RUN" and planned_km and actual_km is not None:
            completion_pct = int(round((actual_km / planned_km) * 100))
        out.append({
            "day": log.workout_date.strftime("%a"),
            "date": log.workout_date,
            "session": log.session_name,
            "planned": f"{int(round(planned_km))} km" if planned_km else ("Gym" if log.workout_type == "STRENGTH" else "Rest"),
            "planned_km": planned_km,
            "actual": f"{actual_km} km" if actual_km is not None else ("Gym" if log.status == "completed" and log.workout_type == "STRENGTH" else None),
            "actual_km": actual_km,
            "done": log.status == "completed",
            "status": log.status,
            "status_label": _status_label(log.status),
            "completion_pct": completion_pct,
            "workout_type": log.workout_type,
            "intensity": planned_day.get("intensity"),
            "importance": planned_day.get("importance"),
            "purpose": planned_day.get("purpose"),
        })
    out = _apply_adaptive_plan(out, today_local, weekly_goal)

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


def _next_key_workout_label(today_local, weekly_plan):
    for item in weekly_plan:
        if item["workout_type"] != "RUN":
            continue
        if item["date"] > today_local and item["status"] == "planned":
            return f"{item['day']} - {item['planned']} {item['session']}"
    return "No upcoming run scheduled this week."


def _next_key_workout_from_plan(today_local, current_plan, next_week_plan=None):
    current_next = _next_key_workout_label(today_local, current_plan)
    if current_next != "No upcoming run scheduled this week.":
        return current_next

    next_week_plan = next_week_plan or []
    future_candidates = [
        item for item in next_week_plan
        if item["workout_type"] == "RUN" and item["status"] == "planned"
    ]
    if not future_candidates:
        return current_next

    high_priority = [item for item in future_candidates if item["session"] in {"Tempo Run", "Long Run", "Aerobic Run"}]
    selected = high_priority[0] if high_priority else future_candidates[0]
    return f"{selected['day']} - {selected['planned']} {selected['session']}"

def _build_today_workout(today_local, runs, weekly_plan, next_key_workout):
    today_iso = today_local.isoformat()
    today_run = next((r for r in runs if r["date"] == today_iso), None)
    today_assignment = next((w for w in weekly_plan if w["date"].isoformat() == today_iso), None)

    workout_name = today_assignment["session"] if today_assignment else "Rest"
    workout_type = today_assignment["workout_type"] if today_assignment else "REST"
    target = today_assignment["planned"] if today_assignment else "Rest"
    planned_km = float(today_assignment["planned_km"] or 0.0) if today_assignment else 0.0

    if today_run:
        actual_km = float(today_run["distance"])
        completion_pct = int(round((actual_km / planned_km) * 100)) if planned_km > 0 else 100
        status_key, _ = _classify_run_completion(actual_km, planned_km)
        return {
            "date": today_local.strftime("%a %b %d"),
            "workout": workout_name,
            "workout_type": workout_type,
            "status": _status_label(status_key),
            "distance_target": target,
            "distance": f"{today_run['distance']} km",
            "distance_actual": f"{today_run['distance']} km",
            "completion_pct": completion_pct,
            "pace": today_run["pace"],
            "hr": str(today_run["hr"]) if today_run["hr"] else "--",
            "coach_insight": "Workout completed for today. Keep recovery and hydration on track.",
            "next_key_workout": next_key_workout,
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
        "pace": "--",
        "hr": "--",
        "coach_insight": "Today's session is assigned from your weekly plan.",
        "next_key_workout": next_key_workout,
        "completed": False,
    }


def _build_ai_summary(intel, weekly_plan):
    # Priority: injury risk -> training imbalance -> missed key session -> long run readiness -> prediction readiness
    bonk_label = (intel.get("bonk_risk", {}).get("label") or "").lower()
    next_run = next((u for u in weekly_plan if u.get("workout_type") == "RUN" and u.get("status") == "planned"), None)
    if intel.get("weekly", {}).get("phase") == "taper":
        if next_run:
            return f"Taper week now. Keep {next_run['day']} {next_run['session']} controlled and protect freshness."
        return "Taper week now. Keep the effort light and arrive at race day fresh."
    if bonk_label == "high":
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

    trend = (intel.get("fitness_trend_label") or "").lower()
    if trend == "declining":
        next_one = next((u for u in upcoming_runs), None)
        if next_one:
            return f"Training momentum is down. Complete {next_one['day']} {next_one['session']} to stabilize load."
        return "Training momentum is down. Complete your next planned run to rebuild consistency."

    next_req = intel.get("training_status", {}).get("next_requirement")
    if next_req:
        return f"Prediction readiness: {next_req}"

    next_long = next((u for u in upcoming_runs if u.get("session") == "Long Run"), None)
    if next_long:
        return f"Training is on track. Nail {next_long['day']} {next_long['planned']} {next_long['session']} this week."
    if next_run:
        return f"Complete {next_run['day']} {next_run['session']} to keep weekly mileage on track."
    return "Training is on track. Stay consistent with your weekly plan."
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

    force_sync = request.args.get("sync") == "1"
    cooldown_min = int(current_app.config.get("STRAVA_SYNC_COOLDOWN_MIN", 15))
    last_sync_at = _parse_iso_datetime(session.get("last_sync_at"))

    if force_sync or _should_sync_now(last_sync_at, cooldown_min):
        sync_info = sync_strava_data(user_id=user.id, pages=current_app.config.get("STRAVA_FETCH_PAGES", 3))
        if sync_info.get("status") == "ok":
            session["last_sync_at"] = datetime.now(timezone.utc).isoformat()
    else:
        sync_info = {"status": "skipped", "reason": "cooldown", "new_activities": 0}

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
    next_key_workout = _next_key_workout_from_plan(today_local, weekly_plan, next_week_plan)
    today_workout = _build_today_workout(today_local, runs, weekly_plan, next_key_workout)
    ai_summary = _build_ai_summary(intel, weekly_plan)
    key_session = _pick_key_session(today_local, weekly_plan) if weekly_plan else None
    key_session_importance = "High" if key_session and key_session["session"] == "Long Run" else "Medium" if key_session and key_session["session"] == "Tempo Run" else "Low"
    planned_runs = [item for item in weekly_plan if item["workout_type"] == "RUN"]
    completed_runs = [item for item in planned_runs if item["status"] == "completed"]
    consistency_score = int(round((len(completed_runs) / max(1, len(planned_runs))) * 100))
    week_closed = _now_local_datetime(user_tz) > datetime.combine(week_end, datetime.max.time()).replace(tzinfo=_now_local_datetime(user_tz).tzinfo)
    weekly_completion_pct = int(round((weekly_goal["completed_km"] / max(1.0, weekly_goal["weekly_goal_km"])) * 100))
    weekly_status = "Goal achieved" if weekly_goal["completed_km"] >= weekly_goal["weekly_goal_km"] else "Goal not achieved" if week_closed else "In progress"
    if key_session is None and next_week_plan:
        key_session = _pick_key_session(week_start + timedelta(days=7), next_week_plan)
        key_session_importance = "High" if key_session and key_session["session"] == "Long Run" else "Medium" if key_session and key_session["session"] == "Tempo Run" else "Low"

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
        goal_confidence_label=intel.get("goal_confidence", "Building"),
        weekly_completion_pct=weekly_completion_pct,
        weekly_status=weekly_status,
        week_closed=week_closed,
        ai_summary=ai_summary,
        runs=runs[:5],
        sync_info=sync_info,
        today_date=_today_date_label(user_tz),
        long_run=intel["long_run"],
    )


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
                )
                return redirect(url_for("web.dashboard"))
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

