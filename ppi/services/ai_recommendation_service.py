"""
Structured coaching output service.

Produces three sections of deterministic data (race prediction, pace strategy,
training recommendations) then uses OpenAI only for the short natural-language
coaching paragraph that stitches them together.
"""
import json

import requests
from flask import current_app

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_pace(sec_per_km: float) -> str:
    """Format seconds-per-km as 'M:SS/km'."""
    sec_per_km = max(0.0, sec_per_km)
    m = int(sec_per_km // 60)
    s = int(sec_per_km % 60)  # truncate, not round, so 339.85 → 5:39 not 5:40
    return f"{m}:{s:02d}/km"


def _fmt_hms(seconds: float) -> str:
    """Format seconds as 'H:MM:SS'."""
    seconds = max(0.0, seconds)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(round(seconds % 60))
    if s == 60:
        m += 1
        s = 0
    return f"{h}:{m:02d}:{s:02d}"


def _parse_hms(s: str) -> float | None:
    """Parse 'H:MM:SS' or 'M:SS' into total seconds."""
    try:
        parts = s.strip().split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, AttributeError):
        pass
    return None


def _riegel(time_sec: float, from_km: float, to_km: float, exponent: float = 1.06) -> float:
    """Riegel cross-distance projection: T2 = T1 × (D2/D1)^exponent."""
    return time_sec * ((to_km / from_km) ** exponent)


# ---------------------------------------------------------------------------
# Section 1 — Race prediction
# ---------------------------------------------------------------------------

def _build_race_prediction(intel: dict) -> dict:
    """Extract and structure the predicted race time from the intel layer.

    For marathon goals the wall-adjusted time is used as the canonical display
    value so the coaching summary always quotes the same figure as the FM tile.
    """
    race_proj = intel.get("race_day_projection", "--")
    current_proj = intel.get("current_projection", "--")
    display = race_proj if race_proj not in ("--", None) else current_proj

    # Use wall-adjusted FM prediction as the single source of truth for display.
    # This matches the FM tile in the dashboard which also shows the wall-adjusted time.
    wall = intel.get("wall_analysis") or {}
    wall_adj = wall.get("current_predicted_fm_display")
    if wall_adj and wall_adj not in ("--", None):
        display = wall_adj

    predicted_seconds = _parse_hms(display) or _parse_hms(current_proj)

    return {
        "predicted_time": display if display not in ("--", None) else "--",
        "predicted_seconds": round(predicted_seconds) if predicted_seconds else None,
        "current_fitness_time": current_proj,
        "confidence": intel.get("prediction_confidence", "Low"),
        "confidence_score": intel.get("prediction_confidence_score", 0.0),
        "note": intel.get("prediction_note", ""),
        "goal_time": (intel.get("goal") or {}).get("goal_time", "--"),
        "gap_to_goal": intel.get("gap_to_goal", "--"),
        "probability": intel.get("probability"),
        "goal_alignment": intel.get("goal_alignment", "--"),
    }


# ---------------------------------------------------------------------------
# Section 2 — Pace strategy
# ---------------------------------------------------------------------------

# Key checkpoint distances (km) per race
_SPLIT_MARKERS = {
    "marathon":      [10.0, 21.1, 30.0, 35.0, 42.195],
    "half_marathon": [5.0, 10.0, 21.0975],
    "10k":           [5.0, 10.0],
    "5k":            [2.5, 5.0],
}

# How much slower (sec/km) the first half should be vs average
_FIRST_HALF_OFFSET = {
    "marathon": 5,
    "half_marathon": 4,
    "10k": 3,
    "5k": 2,
}

# How much faster (sec/km) than average triggers a blowup
_RED_LINE_BUFFER = {
    "marathon": 10,
    "half_marathon": 8,
    "10k": 6,
    "5k": 4,
}


def _pace_strategy_for_distance(key: str, pred_secs: float, dist_km: float) -> dict:
    avg_pace = pred_secs / dist_km
    offset = _FIRST_HALF_OFFSET[key]
    first_half_pace = avg_pace + offset
    second_half_pace = avg_pace - offset   # guarantees negative split at same avg
    red_line_pace = avg_pace - _RED_LINE_BUFFER[key]
    red_line_pace = max(red_line_pace, avg_pace * 0.90)  # cap: never faster than 90% of avg pace

    splits = []
    for km in _SPLIT_MARKERS[key]:
        elapsed = km * avg_pace
        splits.append({"km": km, "target_elapsed": _fmt_hms(elapsed)})

    return {
        "predicted_time": _fmt_hms(pred_secs),
        "predicted_seconds": round(pred_secs),
        "avg_pace": _fmt_pace(avg_pace),
        "first_half_pace": _fmt_pace(first_half_pace),
        "second_half_pace": _fmt_pace(second_half_pace),
        "red_line_pace": _fmt_pace(red_line_pace),
        "splits": splits,
    }


def build_pace_strategy(
    predicted_marathon_seconds: float | None,
    all_distances: dict | None = None,
    goal_seconds: float | None = None,
) -> dict:
    """Derive pace strategy for all four distances.

    For the marathon tile the pace targets are built from *goal_seconds* so
    the runner sees splits that reflect what they must run to hit their goal,
    not the current fitness projection.  Shorter distances use VDOT-accurate
    times from *all_distances*, falling back to Riegel from predicted FM.
    """
    if not predicted_marathon_seconds or predicted_marathon_seconds <= 0:
        return {}

    fm_pace_base = goal_seconds if goal_seconds and goal_seconds > 0 else predicted_marathon_seconds
    ad = all_distances or {}
    hm  = float(ad.get("half_marathon") or 0) or _riegel(predicted_marathon_seconds, 42.195, 21.0975)
    ten = float(ad.get("10k")           or 0) or _riegel(predicted_marathon_seconds, 42.195, 10.0)
    five= float(ad.get("5k")            or 0) or _riegel(predicted_marathon_seconds, 42.195, 5.0)

    return {
        "marathon":      _pace_strategy_for_distance("marathon",      fm_pace_base, 42.195),
        "half_marathon": _pace_strategy_for_distance("half_marathon", hm,           21.0975),
        "10k":           _pace_strategy_for_distance("10k",           ten,          10.0),
        "5k":            _pace_strategy_for_distance("5k",            five,         5.0),
    }


# ---------------------------------------------------------------------------
# Section 3 — Training recommendations
# ---------------------------------------------------------------------------

def _build_training_recommendations(intel: dict) -> dict:
    """Derive training recommendations from CTL/ATL/TSB thresholds."""
    ctl = float(intel.get("current_ctl") or 0.0)
    atl = float(intel.get("current_atl") or 0.0)
    tsb = float(intel.get("current_tsb") or 0.0)

    long_run = intel.get("long_run") or {}
    weekly = intel.get("weekly") or {}
    goal = intel.get("goal") or {}

    prior_avg_km = float(weekly.get("prior_avg_km") or 0.0)
    next_long_km = float(long_run.get("next_milestone_km") or 24.0)

    goal_seconds = float(goal.get("goal_seconds") or 0.0)
    goal_dist_km = float(goal.get("distance_km") or 42.195)
    marathon_pace = (goal_seconds / goal_dist_km) if goal_seconds > 0 else 0.0

    # Phase classification (evaluated in priority order)
    recovery_needed = tsb < -15
    if recovery_needed:
        phase_label = "Recovery week needed"
        phase_rule = "Cap mileage at 60% of last week."
        weekly_target_km = round(prior_avg_km * 0.60, 1)
    elif ctl < 40:
        phase_label = "Base building phase"
        phase_rule = "Prioritize easy runs, one long run per week."
        weekly_target_km = round(max(prior_avg_km, ctl * 1.3), 1)
    elif ctl <= 60:
        phase_label = "Development phase"
        phase_rule = "Add one tempo session per week."
        weekly_target_km = round(max(prior_avg_km, ctl * 1.2), 1)
    else:
        phase_label = "Race ready"
        phase_rule = "Begin taper 3 weeks before race date."
        weekly_target_km = round(prior_avg_km * 0.85, 1)

    # Canonical override: use the same weekly_goal_km that drives the weekly plan
    # so the coaching summary always quotes the same number as the plan card.
    canonical_km = float(weekly.get("weekly_goal_km") or 0.0)
    if canonical_km > 0:
        weekly_target_km = canonical_km

    # Key workout recommendation
    if recovery_needed:
        key_workout = {
            "name": "Recovery Run",
            "description": "30–40 min at very easy, conversational pace. No quality work this week.",
        }
    elif ctl < 40:
        key_workout = {
            "name": "Easy Long Run",
            "description": f"{round(next_long_km)} km at conversational pace. Focus on time on feet, not speed.",
        }
    elif ctl <= 60:
        if marathon_pace > 0:
            lt_pace = marathon_pace * 0.95 + 15  # LT ≈ HM pace + 15 sec/km
            pace_str = _fmt_pace(lt_pace)
            key_workout = {
                "name": "Tempo Run",
                "description": f"6–10 km at threshold pace ({pace_str} target). Comfortably hard — controlled breathing.",
            }
        else:
            key_workout = {
                "name": "Tempo Run",
                "description": "6–10 km at threshold pace. Comfortably hard — controlled breathing throughout.",
            }
    else:
        if marathon_pace > 0:
            mp_str = _fmt_pace(marathon_pace)
            key_workout = {
                "name": "Marathon Pace Run",
                "description": f"12–16 km with 8–10 km at marathon pace ({mp_str}). Build race-specific confidence.",
            }
        else:
            key_workout = {
                "name": "Marathon Pace Run",
                "description": "12–16 km with 8–10 km at marathon pace. Build race-specific confidence.",
            }

    return {
        "phase_label": phase_label,
        "phase_rule": phase_rule,
        "recovery_needed": recovery_needed,
        "next_long_run_km": next_long_km,
        "weekly_target_km": weekly_target_km,
        "key_workout": key_workout,
        "ctl": round(ctl, 1),
        "atl": round(atl, 1),
        "tsb": round(tsb, 1),
    }


# ---------------------------------------------------------------------------
# Coaching summary — OpenAI or heuristic
# ---------------------------------------------------------------------------

def _heuristic_coaching_summary(
    race_prediction: dict,
    training_recs: dict,
    intel: dict,
) -> str:
    """Rule-based coaching paragraph when OpenAI is unavailable."""
    phase_label = training_recs["phase_label"]
    key_workout = training_recs["key_workout"]
    tsb = training_recs["tsb"]
    predicted_time = race_prediction.get("predicted_time", "--")
    confidence = race_prediction.get("confidence", "Low")
    long_run = intel.get("long_run") or {}
    next_long = training_recs["next_long_run_km"]

    if training_recs["recovery_needed"]:
        return (
            f"{phase_label}. Your TSB is {tsb:+.0f}, indicating accumulated fatigue. "
            f"Cap this week at {training_recs['weekly_target_km']} km and focus on the "
            f"{key_workout['name'].lower()} to rebuild freshness before your next quality block."
        )

    goal_alignment = race_prediction.get("goal_alignment") or ""
    time_note = f"Current projection: {predicted_time} ({confidence} confidence)." if predicted_time != "--" else ""

    return (
        f"{phase_label}. {training_recs['phase_rule']} "
        f"{time_note} "
        f"Target {training_recs['weekly_target_km']} km this week with "
        f"{round(next_long)} km on the long run. "
        f"Key session: {key_workout['name']} — {key_workout['description']}"
    ).strip()


def _openai_coaching_summary(
    race_prediction: dict,
    pace_strategy: dict,
    training_recs: dict,
    api_key: str,
    model: str,
) -> str | None:
    """Call OpenAI with structured context; request only a coaching paragraph."""
    context = {
        "race_prediction": {
            "predicted_time": race_prediction.get("predicted_time"),
            "confidence": race_prediction.get("confidence"),
            "goal_time": race_prediction.get("goal_time"),
            "gap_to_goal": race_prediction.get("gap_to_goal"),
            "goal_alignment": race_prediction.get("goal_alignment"),
        },
        "marathon_pace_strategy": pace_strategy.get("marathon", {}),
        "training": {
            "phase_label": training_recs["phase_label"],
            "phase_rule": training_recs["phase_rule"],
            "ctl": training_recs["ctl"],
            "atl": training_recs["atl"],
            "tsb": training_recs["tsb"],
            "weekly_target_km": training_recs["weekly_target_km"],
            "next_long_run_km": training_recs["next_long_run_km"],
            "key_workout": training_recs["key_workout"],
        },
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "input": [
                    {
                        "role": "system",
                        "content": (
                            "You are an elite endurance running coach. "
                            "Write exactly one coaching paragraph (2–3 sentences, plain text, no markdown). "
                            "Be specific, actionable, and encouraging. "
                            "Reference the athlete's actual predicted time, TSB, and key workout."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(context),
                    },
                ],
                "max_output_tokens": 150,
            },
            timeout=20,
        )
    except requests.RequestException:
        return None

    if response.status_code >= 400:
        return None

    text = (response.json() or {}).get("output_text")
    return text.strip() if text else None


# ---------------------------------------------------------------------------
# Primary public API
# ---------------------------------------------------------------------------

def generate_coaching_output(intel: dict, weekly_plan: list) -> dict:
    """Build structured coaching output with 3 deterministic sections plus a
    natural-language paragraph from OpenAI (or heuristic fallback).

    Returns:
        {
            "race_prediction":        {...},
            "pace_strategy":          {"marathon": {...}, "half_marathon": {...}, ...},
            "training_recommendations": {...},
            "coaching_summary":       "...",   # plain text, 2-3 sentences
        }
    """
    # Compute plan total km from the actual weekly plan rows.
    # This overrides the analytics-computed weekly_goal_km so the coaching
    # summary always quotes the same figure as the weekly plan card.
    plan_run_km = round(sum(
        float(item.get("planned_km") or 0.0)
        for item in weekly_plan
        if item.get("workout_type") == "RUN" and item.get("session") != "Race Day"
    ), 1)
    if plan_run_km > 0:
        intel = dict(intel)
        intel["weekly"] = dict(intel.get("weekly") or {})
        intel["weekly"]["weekly_goal_km"] = plan_run_km

    # Also override next_milestone_km in long_run with the actual Sunday planned km
    # so the key workout description matches the plan card.
    sunday_item = next(
        (item for item in weekly_plan if item.get("session") == "Long Run" and item.get("workout_type") == "RUN"),
        None,
    )
    if sunday_item and sunday_item.get("planned_km"):
        intel["long_run"] = dict(intel.get("long_run") or {})
        intel["long_run"]["next_milestone_km"] = float(sunday_item["planned_km"])

    race_prediction = _build_race_prediction(intel)
    goal_ctx = intel.get("goal") or {}
    goal_secs = float(goal_ctx.get("goal_seconds") or 0) or None
    pace_strategy = build_pace_strategy(
        race_prediction.get("predicted_seconds"),
        all_distances=intel.get("all_distances"),
        goal_seconds=goal_secs,
    )
    training_recs = _build_training_recommendations(intel)

    api_key = None
    model = "gpt-4.1-mini"
    try:
        api_key = current_app.config.get("OPENAI_API_KEY")
        model = current_app.config.get("OPENAI_MODEL", "gpt-4.1-mini")
    except RuntimeError:
        pass  # outside app context (e.g. tests)

    if api_key:
        summary = _openai_coaching_summary(race_prediction, pace_strategy, training_recs, api_key, model)
    else:
        summary = None

    if not summary:
        summary = _heuristic_coaching_summary(race_prediction, training_recs, intel)

    wall_analysis = intel.get("wall_analysis")

    return {
        "race_prediction": race_prediction,
        "pace_strategy": pace_strategy,
        "training_recommendations": training_recs,
        "coaching_summary": summary,
        "wall_analysis": wall_analysis,
    }


# ---------------------------------------------------------------------------
# Backward-compatible wrapper (legacy signature — never called in production)
# ---------------------------------------------------------------------------

def generate_ai_recommendation(goal, intel, milestone, recent_metrics):
    """Legacy entry point. Returns the coaching summary string."""
    # Build a minimal intel-shaped dict from the legacy arguments
    minimal_intel = {
        "current_projection": intel.get("current_projection") if isinstance(intel, dict) else "--",
        "race_day_projection": intel.get("race_day_projection") if isinstance(intel, dict) else "--",
        "prediction_confidence": intel.get("prediction_confidence", "Low") if isinstance(intel, dict) else "Low",
        "prediction_confidence_score": intel.get("prediction_confidence_score", 0.0) if isinstance(intel, dict) else 0.0,
        "current_ctl": intel.get("current_ctl", 0.0) if isinstance(intel, dict) else 0.0,
        "current_atl": intel.get("current_atl", 0.0) if isinstance(intel, dict) else 0.0,
        "current_tsb": intel.get("tsb", 0.0) if isinstance(intel, dict) else 0.0,
        "gap_to_goal": intel.get("gap_to_goal", "--") if isinstance(intel, dict) else "--",
        "goal_alignment": intel.get("goal_alignment", "--") if isinstance(intel, dict) else "--",
        "long_run": {"next_milestone_km": milestone.get("longest_km", 24) + 2 if milestone else 24},
        "goal": goal if isinstance(goal, dict) else {},
        "weekly": {},
    }
    result = generate_coaching_output(minimal_intel, [])
    return result["coaching_summary"]
