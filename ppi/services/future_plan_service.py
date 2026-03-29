"""
future_plan_service.py
──────────────────────
Pure planning helpers for deterministic future-week previews, long-run
progressions, and adaptive goal-band guardrails.

No Flask request/session context used here — all functions take plain
Python arguments and return plain Python data structures.
"""
from datetime import timedelta

from flask import current_app  # used only for current_app.logger; safe to import

from ..repositories import fetch_activities_between
from .plan_engine import (
    build_progression_weeks as service_build_progression_weeks,
    effective_long_run_base_km as service_effective_long_run_base_km,
    prescribed_long_run_km as service_prescribed_long_run_km,
    quality_session_prescription as service_quality_session_prescription,
)


# ---------------------------------------------------------------------------
# Adaptive goal-band table
# ---------------------------------------------------------------------------

# (max_goal_seconds, min_weekly_km, max_weekly_km, lr_cap, required_weekly_km)
_ADAPTIVE_GOAL_BANDS = [
    (10_800,  70, 120, 35, 90),   # sub-3:00
    (12_600,  55,  95, 32, 75),   # sub-3:30
    (14_400,  42,  75, 30, 58),   # sub-4:00
    (16_200,  35,  62, 28, 48),   # sub-4:30
    (18_000,  28,  52, 26, 40),   # sub-5:00
    (99_999,  20,  42, 24, 32),   # 5:00+
]


def _get_adaptive_goal_band(goal_seconds):
    """Return guardrail bounds for the athlete's goal time."""
    for max_sec, min_km, max_km, lr_cap, req_km in _ADAPTIVE_GOAL_BANDS:
        if goal_seconds < max_sec:
            return {"min_km": min_km, "max_km": max_km, "lr_cap": lr_cap, "required_km": req_km}
    return {"min_km": 20, "max_km": 42, "lr_cap": 24, "required_km": 32}


# ---------------------------------------------------------------------------
# Display helper
# ---------------------------------------------------------------------------

def _display_planned_km(km):
    return int(round(float(km or 0.0))) if float(km or 0.0) > 0 else 0


# ---------------------------------------------------------------------------
# Trailing actuals
# ---------------------------------------------------------------------------

def _compute_trailing_actuals(user_id, current_week_start, user_tz, n_weeks=4):
    """
    Fetch trailing N completed weeks of run-activity km data.

    Returns a dict with:
      avg_km          -- mean weekly run km across active weeks
      avg_long_km     -- mean longest run per week across active weeks
      n_active_weeks  -- number of weeks with >= 5 km (filters out illness/injury gaps)

    Signal-filter: caller should only apply adaptation when n_active_weeks >= 2.
    """
    _RUN_TYPES = {"run", "trail run", "trail_run", "trailrun", "track", "virtualrun", "treadmill"}
    weeks = []
    for i in range(1, n_weeks + 1):
        w_start = current_week_start - timedelta(weeks=i)
        w_end   = w_start + timedelta(days=6)
        try:
            acts = fetch_activities_between(user_id, w_start, w_end)
        except Exception:
            acts = []
        run_kms = [
            float(a.distance_km or 0)
            for a in acts
            if (a.activity_type or "").lower().replace(" ", "") in {t.replace(" ", "") for t in _RUN_TYPES}
            and float(a.distance_km or 0) > 0
        ]
        total_km = sum(run_kms)
        long_km  = max(run_kms, default=0.0)
        weeks.append({"total_km": round(total_km, 1), "long_km": round(long_km, 1)})

    # Exclude illness/injury weeks (< 5 km = essentially no running that week)
    active = [w for w in weeks if w["total_km"] >= 5]
    if not active:
        return {"avg_km": 0.0, "avg_long_km": 0.0, "n_active_weeks": 0}

    return {
        "avg_km":          round(sum(w["total_km"] for w in active) / len(active), 1),
        "avg_long_km":     round(sum(w["long_km"]  for w in active) / len(active), 1),
        "n_active_weeks":  len(active),
    }


# ---------------------------------------------------------------------------
# Progression weeks
# ---------------------------------------------------------------------------

def _deterministic_progression_weeks(intel, week_start, current_week_weekly_target_km, current_week_long_run_km, weeks=18, schedule_prefs=None):
    from datetime import datetime  # local import to keep module import list clean
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


# ---------------------------------------------------------------------------
# Long-run progression ladder
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Future week preview (adaptive)
# ---------------------------------------------------------------------------

def _deterministic_future_week_preview(intel, week_start, current_week_weekly_target_km, current_week_long_run_km, limit=3, schedule_prefs=None, trailing_actuals=None):
    progression, weekly_goal, race_date_obj = _deterministic_progression_weeks(
        intel,
        week_start,
        current_week_weekly_target_km,
        current_week_long_run_km,
        weeks=max(6, limit + 2),
        schedule_prefs=schedule_prefs,
    )

    # ── Adaptive planning context ────────────────────────────────────────────
    # Resolve goal band and trailing-actual averages once for the full loop.
    # Adaptation is ONLY applied when:
    #   (a) we have >= 2 active trailing weeks (signal filter — ignore noise)
    #   (b) the week is NOT in taper (taper is sacred, never overridden)
    _goal_secs     = float((intel.get("goal") or {}).get("goal_seconds") or 0)
    _goal_band     = _get_adaptive_goal_band(_goal_secs) if _goal_secs > 0 else None
    _trailing_avg  = float((trailing_actuals or {}).get("avg_km") or 0)
    _trailing_lr   = float((trailing_actuals or {}).get("avg_long_km") or 0)
    _n_data_weeks  = int((trailing_actuals or {}).get("n_active_weeks") or 0)
    _can_adapt     = _goal_band is not None and _trailing_avg > 0 and _n_data_weeks >= 2
    # Tracks the previous adapted week target so the 10% cap compounds
    # week-over-week rather than being pegged to a static trailing average.
    _prev_adapted_km = _trailing_avg if _trailing_avg > 0 else float(current_week_weekly_target_km or 0)

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

        # ── Guardrail-bounded adaptive override (non-taper weeks only) ───────
        _weeks_to_race = (
            max(0, (race_date_obj - week["week_start"]).days // 7)
            if race_date_obj else 99
        )
        # Normalise: "recovery week" → "recovery", "cutback week" → "cutback"
        _week_type = (week.get("week_type") or "build").lower().replace(" week", "").strip()
        _is_taper  = _weeks_to_race <= 4 or week.get("phase") == "taper"

        if _can_adapt and not _is_taper:
            # Adaptive base: 50% actuals · 30% goal requirement · 20% plan template
            _adaptive_base = (
                _trailing_avg            * 0.50
                + _goal_band["required_km"] * 0.30
                + weekly_target          * 0.20
            )
            # Week-type modifier
            if _week_type in ("cutback", "recovery", "rebuild"):
                _adapted_km = _adaptive_base * 0.83
            elif _week_type == "peak":
                _adapted_km = _adaptive_base * 1.05
            else:
                _adapted_km = _adaptive_base

            # Structural guardrails
            # 10% cap compounds week-over-week (not from a fixed trailing avg)
            _adapted_km = min(_adapted_km, _prev_adapted_km * 1.10)
            # Recovery weeks get a relaxed floor so they can actually recover
            _floor = (
                _goal_band["min_km"] * 0.70
                if _week_type in ("cutback", "recovery", "rebuild")
                else _goal_band["min_km"]
            )
            _adapted_km = max(_adapted_km, _floor)
            _adapted_km = min(_adapted_km, _goal_band["max_km"])   # goal ceiling
            weekly_target = round(_adapted_km)
            _prev_adapted_km = weekly_target   # carry forward for next week's cap

            # Long run: more conservative blend (60% trailing, 40% plan)
            if _trailing_lr > 0:
                _lr_base = _trailing_lr * 0.60 + snapped_long_run_km * 0.40
                if _week_type in ("cutback", "recovery", "rebuild"):
                    snapped_long_run_km = max(round(_lr_base * 0.83), 10)
                else:
                    snapped_long_run_km = round(min(_lr_base * 1.02, _goal_band["lr_cap"]))
                snapped_long_run_km = min(snapped_long_run_km, _goal_band["lr_cap"])

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
                "all_run_days": [
                    {
                        "session": d.get("session"),
                        "km": _display_planned_km(d.get("target_km") or 0.0),
                        "is_long": d.get("session") == "Long Run",
                        "is_quality": d.get("session") in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"},
                    }
                    for d in sorted(
                        run_days,
                        key=lambda d: (
                            0 if d.get("session") == "Long Run"
                            else 1 if d.get("session") in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Steady Run"}
                            else 2
                        ),
                    )
                ],
            }
        )
        if len(preview) >= limit:
            break
    return preview


# ---------------------------------------------------------------------------
# Upcoming long runs merger
# ---------------------------------------------------------------------------

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
