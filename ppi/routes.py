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

# Service-layer extractions — pure planning logic lives here now
from .services.future_plan_service import (  # noqa: E402
    _ADAPTIVE_GOAL_BANDS,
    _get_adaptive_goal_band,
    _display_planned_km,
    _compute_trailing_actuals,
    _deterministic_progression_weeks,
    _deterministic_long_run_progression,
    _deterministic_future_week_preview,
    _build_upcoming_long_runs,
)


@web.route("/healthz")
def healthz():
    return {"status": "ok"}, 200


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _user_timezone_name():
    return current_app.config.get("USER_TIMEZONE") or current_app.config.get("APP_TIMEZONE") or "Asia/Kolkata"

# ---------------------------------------------------------------------------
# Current-week helpers — extracted to services/current_week_service.py
# ---------------------------------------------------------------------------
from .services.current_week_service import (  # noqa: E402
    _today_date_label,
    _get_coaching_plan_row,
    _load_coaching_freeze_state,
    _save_coaching_freeze_state,
    _load_or_create_weekly_snapshot,
    _session_type_from_template_session,
    _snapshot_needs_schedule_repair,
    _replace_weekly_snapshot,
    _repair_snapshot_zero_long_run,
    _secs_to_pace_str,
    _pace_guidance_for_session,
    _schedule_preferences_from_profile,
    _deterministic_current_week_daily_plan,
    _infer_session_type_from_log,
    _build_current_week_coaching_message,
    _derive_current_week_display_metrics,
    _format_alternate_activity_text,
    _build_session_verdict,
    _different_activity_status_label,
    _deterministic_phase_label,
    _deterministic_feasibility_fields,
    _persist_snapshot_workout_logs,
    _parse_iso_datetime,
    _should_sync_now,
    fix_coaching_numbers,
    _today_local_date,
    _now_local_datetime,
    _week_bounds,
    _activity_local_date,
    _weekly_plan_template,
    _classify_run_completion,
    _status_label,
    _goal_marathon_pace,
    _run_pace_sec_per_km,
    _select_best_run_for_session,
    _classify_quality_completion,
    _priority_rank,
    _plan_meta_for_session,
    _fatigue_score,
    _apply_adaptive_plan,
    _build_weekly_plan,
    _pick_key_session,
    _next_upcoming_run_label,
    _next_upcoming_run_from_plan,
    _build_today_workout,
    _build_ai_summary,
    _training_consistency_score,
)


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
    # Override intel with the frozen weekly snapshot's canonical km values so
    # _build_weekly_plan and generate_coaching_output use the same figures as
    # the weekly plan card (not the analytics-derived pipeline values which can
    # lag behind the snapshot, e.g. long_run next_milestone_km = 14 when the
    # snapshot correctly shows 15).
    week_start, _ = _week_bounds(today_local)
    _api_plan_row = _get_coaching_plan_row(user.id)
    _api_freeze = _load_coaching_freeze_state(_api_plan_row)
    _api_snapshot = (_api_freeze.get("weekly_plan_snapshots") or {}).get(week_start.isoformat())
    if _api_snapshot:
        # Sum individual day distances (matches what the weekly plan card shows)
        # rather than reading the stored weekly_target_km field which can be
        # 1 km lower due to plan-engine distribution rounding.
        _snap_days = (_api_snapshot.get("days") or {}).values()
        _snap_wk_km = round(sum(
            float(d.get("planned_distance_km") or 0.0)
            for d in _snap_days
            if d.get("workout_type") == "RUN"
        ), 1)
        _snap_lr_km = max(
            (
                float(d.get("planned_distance_km") or 0.0)
                for d in (_api_snapshot.get("days") or {}).values()
                if d.get("session_name") == "Long Run"
            ),
            default=0.0,
        )
        if _snap_wk_km > 0 or _snap_lr_km > 0:
            intel = dict(intel)
            if _snap_wk_km > 0:
                intel["weekly"] = dict(intel.get("weekly") or {})
                intel["weekly"]["weekly_goal_km"] = _snap_wk_km
            if _snap_lr_km > 0:
                intel["long_run"] = dict(intel.get("long_run") or {})
                intel["long_run"]["next_milestone_km"] = _snap_lr_km

    weekly_plan = _build_weekly_plan(
        user.id, today_local, user_tz,
        intel["weekly"], intel["long_run"],
        week_start=week_start,
        persist=False,
    )
    coaching = generate_coaching_output(intel, weekly_plan)

    # ── Step 3b: Sanitise coaching summary numbers ───────────────────────────
    # The AI / heuristic may quote values that are slightly off (e.g. the
    # analytics pipeline's 42.0 vs the snapshot's canonical 43 km).  Apply
    # fix_coaching_numbers here using the same snapshot-derived values so the
    # coaching card always agrees with the weekly plan card.
    _canonical_wk = _snap_wk_km if (_api_snapshot and _snap_wk_km > 0) else float(intel.get("weekly", {}).get("weekly_goal_km") or 0)
    _canonical_lr = _snap_lr_km if (_api_snapshot and _snap_lr_km > 0) else float(intel.get("long_run", {}).get("next_milestone_km") or 0)
    _raw_summary = coaching["coaching_summary"] or ""
    if _canonical_wk > 0 or _canonical_lr > 0:
        _raw_summary = fix_coaching_numbers(_raw_summary, _canonical_wk, _canonical_lr)

    # ── Step 4: Single JSON response with all 3 outputs ─────────────────────
    return jsonify({
        "load": load_output,
        "prediction": prediction_output,
        "pace_strategy": coaching["pace_strategy"],
        "training_recommendations": coaching["training_recommendations"],
        "coaching_summary": _raw_summary,
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


# Auth, settings, onboarding, coach-intro, and Strava OAuth routes have been
# moved to routes_auth.py (imported by __init__.py to register them on web).



@web.route("/sync", methods=["POST"])
@login_required
@limiter.limit("4 per minute")
def sync_strava():
    """Explicit sync action — POST only, never triggered by a GET page load."""
    user = _current_user()
    sync_info = sync_strava_data(
        user_id=user.id,
        pages=current_app.config.get("STRAVA_FETCH_PAGES", 3),
    )
    if sync_info.get("status") == "ok":
        session["last_sync_at"] = datetime.now(timezone.utc).isoformat()
    return redirect(url_for("web.dashboard"))


@web.route("/")
@login_required
def dashboard():
    return _dashboard_inner()


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

    # Auto-sync on cooldown — passive background refresh, not triggered by URL param.
    # Explicit syncs go through POST /sync.
    cooldown_min = int(current_app.config.get("STRAVA_SYNC_COOLDOWN_MIN", 15))
    last_sync_at = _parse_iso_datetime(session.get("last_sync_at"))

    if _should_sync_now(last_sync_at, cooldown_min):
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
    weekly_snapshot, _ = _repair_snapshot_zero_long_run(_plan_row, week_start, weekly_snapshot, _daily_plan)
    _persist_snapshot_workout_logs(user.id, weekly_snapshot)
    canonical_weekly_target_km = round(float(weekly_snapshot.get("weekly_target_km") or 0.0), 1)
    canonical_long_run_km = round(
        max(
            [
                float(day.get("planned_distance_km") or 0.0)
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

    # ── Fallback: use wall_analysis direct prediction when no race prediction ─
    _wall_fm_display = _wall_data.get('current_predicted_fm_display') or ''
    _wall_fm_sec     = float(_wall_data.get('current_predicted_fm_seconds') or 0)
    if not _wall_fm_sec and _wall_fm_display:
        # parse "H:MM:SS" → seconds
        try:
            _p = _wall_fm_display.split(':')
            _wall_fm_sec = int(_p[0]) * 3600 + int(_p[1]) * 60 + int(_p[2])
        except Exception:
            _wall_fm_sec = 0
    if not canonical_wall_base_display and _wall_fm_display:
        canonical_wall_base_display = _wall_fm_display
    if not canonical_wall_adjusted_display and _wall_fm_display:
        canonical_wall_adjusted_display = _wall_fm_display
    if not canonical_wall_adjusted_seconds and _wall_fm_sec:
        canonical_wall_adjusted_seconds = int(_wall_fm_sec)

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

    # ── FM gap fallback: use wall-adjusted seconds when race-prediction path misses ──
    if not fm_gap_display and canonical_wall_adjusted_seconds > 0 and _goal_secs > 0:
        _wall_gap_sec = canonical_wall_adjusted_seconds - _goal_secs
        _wall_gap_min = abs(int(_wall_gap_sec // 60))
        if _wall_gap_sec > 30:
            fm_gap_display = f"+{_wall_gap_min} min from goal"
            fm_gap_color   = "amber"
        elif _wall_gap_sec < -30:
            fm_gap_display = f"{_wall_gap_min} min under goal"
            fm_gap_color   = "green"
        else:
            fm_gap_display = "On goal pace"
            fm_gap_color   = "green"

    # ── Limiting factor (lowest readiness subscore) ───────────────────────────
    _factor_scores_raw = {
        "Long Run":    intel.get("readiness_long_run_pct", 0),
        "Volume":      intel.get("readiness_volume_pct", 0),
        "Consistency": intel.get("readiness_consistency_pct", 0),
        "Fitness":     intel.get("readiness_fitness_pct", 0),
    }
    limiting_factor      = min(_factor_scores_raw, key=_factor_scores_raw.get)
    limiting_factor_pct  = _factor_scores_raw[limiting_factor]

    # ── Taper countdown ───────────────────────────────────────────────────────
    _d2r          = int((intel.get("goal") or {}).get("days_remaining") or 0)
    _taper_days   = max(0, _d2r - 21)           # typical 3-week taper
    weeks_to_taper = round(_taper_days / 7, 1)

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
            "done": item["state"] in {DONE, OVERDONE},
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

    # Refresh pace_guidance with canonical goal pace — snapshot value may be
    # stale (created before goal_marathon_pace_sec_per_km was available).
    _canonical_goal_pace_sec = float((intel.get("weekly") or {}).get("goal_marathon_pace_sec_per_km") or 0.0)
    for item in weekly_plan:
        if item.get("workout_type") == "RUN":
            item["pace_guidance"] = _pace_guidance_for_session(item["session"], _canonical_goal_pace_sec)

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
        "verdict":           _build_session_verdict(today_item),
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
            "pace_sec": round(_a.moving_time / _a.distance_km) if (_a.distance_km and _a.distance_km > 0 and _a.moving_time) else None,
        })
        # Tag easy vs hard based on goal pace threshold
        _ps = recent_activities[-1]["pace_sec"]
        _is_run = recent_activities[-1]["is_run"]
        recent_activities[-1]["is_hard"] = bool(
            _is_run and _ps and _canonical_goal_pace_sec > 0
            and _ps <= _canonical_goal_pace_sec * 1.05
        )
        recent_activities[-1]["intensity_label"] = (
            "hard" if recent_activities[-1]["is_hard"]
            else ("easy" if _is_run and _ps else None)
        )

    # Compute pace trend — dedicated run-only query so walks/strength don't eat
    # into the 7-activity window and starve us of run data.
    _RUN_TYPE_VARIANTS = {"run", "virtualrun", "trail run", "trail_run", "treadmill", "track"}
    _pace_trend_acts = (
        Activity.query
        .filter(Activity.user_id == user.id)
        .order_by(Activity.date.desc())
        .limit(30)
        .all()
    )
    _run_paces = [
        round(a.moving_time / a.distance_km)
        for a in _pace_trend_acts
        if (a.activity_type or "").lower() in _RUN_TYPE_VARIANTS
        and a.moving_time and a.distance_km
        and 0 < float(a.distance_km) <= 16   # exclude long runs — their intentionally slow pace skews trend
    ][:5]  # use the 5 most recent qualifying runs
    _pace_trend = None  # "improving", "steady", "slowing"
    if len(_run_paces) >= 2:
        _latest = _run_paces[0]
        _prior_avg = sum(_run_paces[1:]) / len(_run_paces[1:])
        _diff_pct = (_latest - _prior_avg) / _prior_avg
        if _diff_pct < -0.02:
            _pace_trend = "improving"  # faster (lower sec/km)
        elif _diff_pct > 0.02:
            _pace_trend = "slowing"
        else:
            _pace_trend = "steady"

    # ── 7-day streak dots ─────────────────────────────────────────────────────
    _today_date = _utcnow_naive().date()
    _seven_ago  = _today_date - timedelta(days=6)
    _streak_raw = (
        Activity.query
        .filter(Activity.user_id == user.id,
                Activity.date >= datetime(_seven_ago.year, _seven_ago.month, _seven_ago.day))
        .all()
    )
    _streak_run_dates = set()
    _streak_any_dates = set()
    for _sa in _streak_raw:
        _sd = _sa.date.date() if hasattr(_sa.date, 'date') else _sa.date
        _streak_any_dates.add(_sd)
        if (_sa.activity_type or "").lower() in _RUN_TYPES:
            _streak_run_dates.add(_sd)
    _DAY_ABBRS7 = ["M", "T", "W", "T", "F", "S", "S"]
    streak_dots = []
    for _si in range(6, -1, -1):
        _sd = _today_date - timedelta(days=_si)
        streak_dots.append({
            "abbr":     _DAY_ABBRS7[_sd.weekday()],
            "has_run":  _sd in _streak_run_dates,
            "has_any":  _sd in _streak_any_dates,
            "is_today": _si == 0,
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

    # ── Trailing actuals for adaptive planning ────────────────────────────────
    _trailing_actuals = {}
    try:
        _trailing_actuals = _compute_trailing_actuals(user.id, week_start, user_tz, n_weeks=4)
        # Boost the trailing average if the current week's actual is stronger —
        # this ensures an over-achiever week immediately lifts next-week targets.
        if week_actual_km > 0:
            _trailing_actuals["avg_km"] = max(
                float(_trailing_actuals.get("avg_km") or 0),
                week_actual_km * 0.88,
            )
            if recent_long_run_km > 0:
                _trailing_actuals["avg_long_km"] = max(
                    float(_trailing_actuals.get("avg_long_km") or 0),
                    recent_long_run_km * 0.90,
                )
            # Ensure at least 2 active weeks so adaptation can trigger
            if _trailing_actuals.get("n_active_weeks", 0) < 2:
                _trailing_actuals["n_active_weeks"] = 2
    except Exception:
        pass  # Never break dashboard over non-critical adaptive data

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
            trailing_actuals=_trailing_actuals,
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
        canonical_feasibility_factor_scores={
            "long_run":    intel.get("readiness_long_run_pct", 0),
            "volume":      intel.get("readiness_volume_pct", 0),
            "consistency": intel.get("readiness_consistency_pct", 0),
            "fitness":     intel.get("readiness_fitness_pct", 0),
        },
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
        pace_trend=_pace_trend,
        streak_dots=streak_dots,
        limiting_factor=limiting_factor,
        limiting_factor_pct=limiting_factor_pct,
        weeks_to_taper=weeks_to_taper,
    )

