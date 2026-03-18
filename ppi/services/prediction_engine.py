"""
Marathon and multi-distance race prediction engine.

Primary method:   Jack Daniels VDOT model
Secondary method: Pete Riegel power law (T2 = T1 × (D2/D1)^1.06)
Training estimate: Riegel projection with effort-adjustment factors
Wall model:       HM→FM VDOT gap drives a km-32 blow-up penalty
"""
import math
from datetime import date as _date

# ---------------------------------------------------------------------------
# Jack Daniels VDOT formula constants
# Reference: Daniels' Running Formula, 3rd ed.
# ---------------------------------------------------------------------------
_JD_VO2_A = -4.60
_JD_VO2_B = 0.182258
_JD_VO2_C = 0.000104
_JD_PCT_P0 = 0.8
_JD_PCT_P1 = 0.1894393
_JD_PCT_K1 = -0.012778
_JD_PCT_P2 = 0.2989558
_JD_PCT_K2 = -0.1932605

# Race distances in metres
_5K_M  = 5_000.0
_10K_M = 10_000.0
_HM_M  = 21_097.5
_FM_M  = 42_195.0

# Wall model: km-32 split (how much the last 10.195 km slow down)
# Overall factor = (32 + 10.195 × km32_factor) / 42.195
_WALL_KM32_FACTOR = {"high": 1.14, "medium": 1.10, "low": 1.03, "unknown": 1.0}
_WALL_OVERALL_FACTOR = {
    k: (32.0 + 10.195 * v) / 42.195
    for k, v in _WALL_KM32_FACTOR.items()
}
# high → ×1.034,  medium → ×1.024,  low → ×1.007,  unknown → ×1.000


# ---------------------------------------------------------------------------
# Core VDOT functions
# ---------------------------------------------------------------------------

def vdot_from_race(distance_m: float, duration_min: float) -> float | None:
    """Calculate VDOT from a race result (Jack Daniels formula)."""
    if duration_min <= 0 or distance_m <= 0:
        return None
    v = distance_m / duration_min
    vo2 = _JD_VO2_A + _JD_VO2_B * v + _JD_VO2_C * v * v
    pct = (
        _JD_PCT_P0
        + _JD_PCT_P1 * math.exp(_JD_PCT_K1 * duration_min)
        + _JD_PCT_P2 * math.exp(_JD_PCT_K2 * duration_min)
    )
    if pct <= 0:
        return None
    return round(vo2 / pct, 2)


def vdot_to_race_time_seconds(vdot: float, distance_m: float) -> float | None:
    """Solve for the race time (seconds) corresponding to a given VDOT.

    Binary search over [1, 600] minutes.  Guaranteed to converge because
    VDOT is a strictly decreasing function of finishing time.
    """
    if vdot <= 0 or distance_m <= 0:
        return None
    lo, hi = 1.0, 600.0
    for _ in range(64):
        mid = (lo + hi) / 2.0
        v = distance_m / mid
        vo2 = _JD_VO2_A + _JD_VO2_B * v + _JD_VO2_C * v * v
        pct = (
            _JD_PCT_P0
            + _JD_PCT_P1 * math.exp(_JD_PCT_K1 * mid)
            + _JD_PCT_P2 * math.exp(_JD_PCT_K2 * mid)
        )
        computed = vo2 / max(1e-9, pct)
        if computed > vdot:
            lo = mid
        else:
            hi = mid
    return ((lo + hi) / 2.0) * 60.0


# ---------------------------------------------------------------------------
# Wall risk model
# ---------------------------------------------------------------------------

def compute_wall_risk(hm_vdot: float | None, fm_vdot: float | None) -> dict:
    """Classify marathon wall risk from the HM→FM VDOT gap.

    A large gap means the runner's aerobic ceiling (measured at HM) far
    exceeds their actual FM performance — a signature of chronic wall-hitting
    due to poor pacing or fueling.

    Classification:
        gap > 8  → "high"   (chronic wall hitter — typically blows up at km 32)
        gap 4–8  → "medium" (moderate fade in the last quarter)
        gap < 4  → "low"    (well-paced; FM close to aerobic potential)

    Args:
        hm_vdot: VDOT derived from a half-marathon result.
        fm_vdot: VDOT derived from a full marathon result.

    Returns:
        dict with keys: risk, gap, hm_vdot, fm_vdot
    """
    if hm_vdot is None or fm_vdot is None:
        return {"risk": "unknown", "gap": None, "hm_vdot": hm_vdot, "fm_vdot": fm_vdot}
    gap = round(hm_vdot - fm_vdot, 2)
    if gap > 8:
        risk = "high"
    elif gap >= 4:
        risk = "medium"
    else:
        risk = "low"
    return {"risk": risk, "gap": gap, "hm_vdot": round(hm_vdot, 2), "fm_vdot": round(fm_vdot, 2)}


def _vdot_by_distance(metrics, today) -> dict:
    """Return best VDOT per distance bucket.

    Primary source: recent_race_runs (is_race=True activities).
    Fallback for HM bucket: any training run of 16–23 km, Riegel-projected
    to 21.0975 km.  This catches HM races mis-labelled as training runs and
    genuine long-effort runs that give a conservative HM VDOT estimate.

    Buckets:  "short" (< 10 km),  "10k" (10–15 km),  "hm" (15–25 km)
    """
    best: dict[str, float] = {}

    # ── Primary: labelled race efforts ───────────────────────────────────
    for run in metrics.get("recent_race_runs", []):
        dist_m = float(run.get("distance_km") or 0.0) * 1000.0
        time_sec = float(run.get("moving_time_sec") or 0.0)
        if dist_m < 3000.0 or time_sec <= 0:
            continue
        v = vdot_from_race(dist_m, time_sec / 60.0)
        if not v or v < 20:
            continue
        run_date = run.get("date")
        if run_date and (today - run_date).days > 180:
            continue
        bucket = "hm" if dist_m >= 15_000 else "10k" if dist_m >= 8_000 else "short"
        if best.get(bucket, 0) < v:
            best[bucket] = v

    # ── Fallback: training runs 16–23 km → estimate HM VDOT via Riegel ──
    # Only used when no race-labelled HM effort was found.
    if "hm" not in best:
        candidates = (
            metrics.get("long_runs", [])
            + metrics.get("marathon_specific_runs", [])
        )
        seen_ids: set = set()
        for run in candidates:
            rid = id(run)
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            dist_km = float(run.get("distance_km") or 0.0)
            if not (16.0 <= dist_km <= 23.0):
                continue
            time_sec = float(run.get("moving_time_sec") or 0.0)
            if time_sec <= 0:
                continue
            run_date = run.get("date")
            if run_date and (today - run_date).days > 180:
                continue
            # Riegel project actual run to HM distance
            hm_proj_sec = time_sec * ((21.0975 / dist_km) ** 1.06)
            v = vdot_from_race(_HM_M, hm_proj_sec / 60.0)
            if v and v >= 20 and best.get("hm", 0) < v:
                best["hm"] = v

    return best


def _best_vdot_from_metrics(metrics, today) -> tuple[float | None, int | None]:
    """Return (best_vdot_overall, days_since_last_race) from recent_race_runs."""
    best_vdot = None
    days_since_last_race = None
    for run in metrics.get("recent_race_runs", []):
        dist_m = float(run.get("distance_km") or 0.0) * 1000.0
        time_sec = float(run.get("moving_time_sec") or 0.0)
        if dist_m < 3000.0 or time_sec <= 0:
            continue
        v = vdot_from_race(dist_m, time_sec / 60.0)
        if not v or v < 20:
            continue
        run_date = run.get("date")
        age = (today - run_date).days if run_date else 999
        if age > 180:
            continue
        if best_vdot is None or v > best_vdot:
            best_vdot = v
        if days_since_last_race is None or age < days_since_last_race:
            days_since_last_race = age
    return best_vdot, days_since_last_race


# ---------------------------------------------------------------------------
# Wall-prevention pace strategy and fueling plan
# ---------------------------------------------------------------------------

def _fmt_pace(sec_per_km: float) -> str:
    sec_per_km = max(0.0, sec_per_km)
    m = int(sec_per_km // 60)
    s = int(round(sec_per_km % 60))
    if s == 60:
        m += 1
        s = 0
    return f"{m}:{s:02d}/km"


def _fmt_hms(seconds: float) -> str:
    seconds = max(0.0, seconds)
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(round(seconds % 60))
    if s == 60:
        m += 1
        s = 0
    return f"{h}:{m:02d}:{s:02d}"


def fm_wall_prevention_pace_strategy(goal_pace_sec_per_km: float, wall_risk: str = "high") -> dict:
    """Build a 4-segment marathon pacing plan designed to prevent the km-32 wall.

    Segment logic (relative to goal pace):
        km  0–10 : goal + 20 s/km  — deliberately conservative opening
        km 10–21 : goal + 10 s/km  — settling in, aerobic lock-on
        km 21–32 : goal            — controlled race-pace effort
        km 32–42 : goal -  5 s/km  — negative-split finish if wall avoided

    km 32 is flagged as a CRITICAL CHECKPOINT with a bail-out protocol.

    Args:
        goal_pace_sec_per_km: Target marathon pace in seconds per km.
        wall_risk:            One of "high", "medium", "low".

    Returns:
        A structured dict with per-segment paces, split times, and the
        km-32 checkpoint advisory.
    """
    p = goal_pace_sec_per_km

    segments = [
        {"label": "km 0–10",  "start_km": 0,   "end_km": 10.0,   "pace": p + 20, "note": "Deliberately conservative. Hold back even if you feel fresh."},
        {"label": "km 10–21", "start_km": 10.0, "end_km": 21.1,   "pace": p + 10, "note": "Settling in. Breathing should be controlled and comfortable."},
        {"label": "km 21–32", "start_km": 21.1, "end_km": 32.0,   "pace": p,      "note": "Race pace. This is where discipline pays off."},
        {"label": "km 32–42", "start_km": 32.0, "end_km": 42.195, "pace": p - 5,  "note": "Negative split. Only attempt if you feel strong at km 32."},
    ]

    # Compute predicted split times
    elapsed = 0.0
    for seg in segments:
        dist = seg["end_km"] - seg["start_km"]
        seg_time = dist * seg["pace"]
        elapsed += seg_time
        seg["segment_time"] = _fmt_hms(seg_time)
        seg["elapsed_at_end"] = _fmt_hms(elapsed)
        seg["pace_display"] = _fmt_pace(seg["pace"])

    predicted_total = elapsed

    # Km-32 bail-out pace (15 sec/km slower than goal)
    bailout_pace = p + 15

    # Red-line pace — if you're averaging faster than this through km 21, blow-up is likely
    redline_pace = p - 10

    return {
        "wall_risk": wall_risk,
        "goal_pace": _fmt_pace(p),
        "predicted_total_time": _fmt_hms(predicted_total),
        "segments": segments,
        "km_32_checkpoint": {
            "label": "CRITICAL CHECKPOINT — km 32",
            "description": (
                "This is where wall-prone runners begin to blow up. "
                "Evaluate your effort level honestly at this marker."
            ),
            "if_strain": (
                f"Drop pace to {_fmt_pace(bailout_pace)} immediately. "
                "Do not push through — glycogen depletion is exponential beyond this point."
            ),
            "if_controlled": (
                f"You are on track for a negative split. Hold {_fmt_pace(p - 5)} "
                "and begin your finish-line push from km 38."
            ),
        },
        "red_line_pace": _fmt_pace(redline_pace),
        "red_line_warning": (
            f"If your average pace through km 21 is faster than {_fmt_pace(redline_pace)}, "
            "you are running the first half too fast. Slow down now."
        ),
    }


def marathon_fueling_plan(predicted_seconds: float, goal_pace_sec_per_km: float) -> dict:
    """Generate a race-day fueling and hydration plan for a wall-risk runner.

    Fueling principles:
      - Start carb intake early (km 10) — before you need it
      - Target 60 g carbs/hour (upper limit of gut absorption for most runners)
      - Gel every 45 min, not every 5 km — time-based is more reliable under fatigue
      - Electrolytes every 10 km from km 10 to prevent cramping and hyponatraemia

    Args:
        predicted_seconds:      Predicted finish time in seconds.
        goal_pace_sec_per_km:   Race pace used to convert time targets to distances.

    Returns:
        dict with gel schedule, hydration rules, and pre-race protocol.
    """
    p = goal_pace_sec_per_km

    def elapsed_at_km(km: float) -> float:
        return km * p

    def km_at_elapsed(t: float) -> float:
        return t / p

    # Gel schedule: every 45 min starting at km 10
    gel_schedule = []
    t = elapsed_at_km(10.0)
    interval = 45 * 60  # 45 minutes in seconds
    gel_num = 1
    while t < predicted_seconds - 300:
        km = round(km_at_elapsed(t), 1)
        gel_schedule.append({
            "gel": gel_num,
            "elapsed": _fmt_hms(t),
            "approx_km": km,
            "note": "Take with 150–200 ml water. Do not take with sports drink — risk of GI distress.",
        })
        t += interval
        gel_num += 1

    # Electrolyte checkpoints every 10 km from km 10
    electrolyte_stops = []
    for km in range(10, 43, 10):
        if km > 42.195:
            break
        electrolyte_stops.append({
            "km": km,
            "elapsed": _fmt_hms(elapsed_at_km(km)),
            "action": "Salt tab or electrolyte drink (not plain water only).",
        })

    total_gels = len(gel_schedule)
    approx_carbs_per_hour = round((total_gels * 25) / (predicted_seconds / 3600), 0)

    return {
        "target_carbs_per_hour_g": 60,
        "gel_count": total_gels,
        "approx_carbs_per_hour_actual_g": approx_carbs_per_hour,
        "gel_note": "Each standard gel ≈ 25 g carbs. Aim for 2–3 gels per hour minimum.",
        "gel_schedule": gel_schedule,
        "electrolyte_stops": electrolyte_stops,
        "hydration": {
            "every_aid_station": "Sip 100–150 ml water at every aid station (~every 2.5 km). Never gulp.",
            "electrolytes": "Alternate water and electrolyte drink from km 10. Never drink only water for the last 20 km.",
            "warning": "Hyponatraemia (over-hydration) is as dangerous as dehydration. Drink to thirst, not on a fixed volume schedule.",
        },
        "pre_race_protocol": {
            "night_before": "400–500 g carbs across dinner + evening snack. No new foods.",
            "morning_of": "200–300 g carbs 2–3 h before start (oats, banana, white toast). 500 ml water. Stop eating 90 min before.",
            "final_30_min": "Optional: 1 gel or energy chew 15 min before start gun for a glycogen top-up.",
        },
        "km_32_fueling_note": (
            "By km 32 you have likely consumed 75–80% of your liver glycogen. "
            "If you skipped any gels before this point, you WILL hit the wall. "
            "There is no recovery — only damage limitation."
        ),
    }


# ---------------------------------------------------------------------------
# Full wall analysis — public entry point
# ---------------------------------------------------------------------------

def marathon_wall_analysis(
    metrics: dict,
    pb_fm_seconds: float | None = None,
    pb_fm_distance_km: float = 42.195,
) -> dict | None:
    """Return a comprehensive wall-risk analysis with two FM predictions.

    Uses:
      - VDOT from recent HM/shorter races as the aerobic ceiling
      - VDOT from pb_fm_seconds (if supplied) or metrics["personal_best_fm_seconds"]
        as the historical FM performance
      - Training-run Riegel estimate as current fitness signal

    Returns:
        {
            "wall_risk":           "high" | "medium" | "low" | "unknown",
            "vdot_gap":            float | None,
            "hm_vdot":             float | None,
            "fm_vdot":             float | None,
            "optimal_fm_potential":  seconds (aerobic ceiling, wall solved),
            "current_predicted_fm":  seconds (realistic, wall accounted for),
            "wall_cost_minutes":     float | None,
            "wall_factor_applied":   float,
            "wall_prevention_pacing": {...},
            "fueling_plan":          {...},
        }
    or None when insufficient data.
    """
    today = _date.today()

    # Resolve FM personal best
    fm_pb_sec = pb_fm_seconds or float(metrics.get("personal_best_fm_seconds") or 0.0) or None
    if fm_pb_sec:
        fm_vdot = vdot_from_race(pb_fm_distance_km * 1000.0, fm_pb_sec / 60.0)
    else:
        fm_vdot = None

    # Best HM VDOT from recent races
    by_dist = _vdot_by_distance(metrics, today)
    hm_vdot = by_dist.get("hm") or by_dist.get("10k") or by_dist.get("short")

    # Wall risk classification
    wall = compute_wall_risk(hm_vdot, fm_vdot)

    # Optimal FM potential = clean VDOT projection from best short race
    best_vdot = hm_vdot
    optimal_secs = vdot_to_race_time_seconds(best_vdot, _FM_M) if best_vdot else None

    # Training-based current fitness estimate (Riegel from long runs)
    training_est = _training_run_estimate(metrics)

    # Current predicted = blended training estimate with wall factor applied
    best_vdot_overall, days_since = _best_vdot_from_metrics(metrics, today)
    vdot_marathon = vdot_to_race_time_seconds(best_vdot_overall, _FM_M) if best_vdot_overall else None

    if vdot_marathon and training_est:
        blended = (
            0.70 * vdot_marathon + 0.30 * training_est if (days_since is not None and days_since < 60)
            else 0.40 * vdot_marathon + 0.60 * training_est
        )
    elif vdot_marathon:
        blended = vdot_marathon
    elif training_est:
        blended = training_est
    else:
        return None

    wall_factor = _WALL_OVERALL_FACTOR[wall["risk"]]
    current_secs = blended * wall_factor

    wall_cost_min = round((current_secs - (optimal_secs or blended)) / 60.0, 1)

    # Goal pace for pacing strategy (use goal or derive from optimal)
    goal_ctx = metrics.get("goal") or {}
    goal_secs = float(goal_ctx.get("goal_seconds") or 0.0)
    goal_dist = float(goal_ctx.get("distance_km") or 42.195)
    if goal_secs > 0:
        goal_pace = goal_secs / goal_dist
    elif optimal_secs:
        goal_pace = optimal_secs / 42.195
    else:
        goal_pace = current_secs / 42.195

    pacing = fm_wall_prevention_pace_strategy(goal_pace, wall["risk"])
    fueling = marathon_fueling_plan(current_secs, goal_pace)

    return {
        "wall_risk": wall["risk"],
        "vdot_gap": wall["gap"],
        "hm_vdot": wall["hm_vdot"],
        "fm_vdot": wall["fm_vdot"],
        "optimal_fm_potential": round(optimal_secs) if optimal_secs else None,
        "optimal_fm_potential_display": _fmt_hms(optimal_secs) if optimal_secs else "--",
        "current_predicted_fm": round(current_secs),
        "current_predicted_fm_display": _fmt_hms(current_secs),
        "wall_cost_minutes": wall_cost_min,
        "wall_factor_applied": round(wall_factor, 4),
        "wall_prevention_pacing": pacing,
        "fueling_plan": fueling,
    }


# ---------------------------------------------------------------------------
# Training-run estimate (private)
# ---------------------------------------------------------------------------

def _training_run_estimate(metrics) -> float | None:
    """Derive marathon estimate from training runs via Riegel + effort factors."""
    signals = []

    for run in metrics.get("race_simulation_runs", []):
        time_sec = run.get("moving_time_sec")
        if time_sec and run["distance_km"] >= 20:
            proj = riegel_projection(time_sec, run["distance_km"], 42.195, 1.03)
            if proj:
                signals.append((proj, 0.36))

    seen: set = set()
    for run in metrics.get("long_runs", []) + metrics.get("marathon_specific_runs", []):
        rid = id(run)
        if rid in seen:
            continue
        seen.add(rid)
        dist = float(run.get("distance_km") or 0.0)
        time_sec = run.get("moving_time_sec")
        if not time_sec or dist < 18.0:
            continue
        fatigue_factor = 1.04 if dist >= 28 else 1.06 if dist >= 22 else 1.08
        proj = riegel_projection(time_sec, dist, 42.195, 1.04)
        if proj:
            weight = 0.28 if dist >= 24 else 0.20
            signals.append((proj * fatigue_factor, weight))

    for run in metrics.get("medium_runs", []):
        if run.get("intensity") not in {"tempo", "speed"}:
            continue
        time_sec = run.get("moving_time_sec")
        if time_sec and run["distance_km"] >= 8:
            proj = riegel_projection(time_sec, run["distance_km"], 42.195, 1.06)
            if proj:
                signals.append((proj, 0.12))

    if not signals:
        return None
    total_w = sum(w for _, w in signals)
    return sum(v * w for v, w in signals) / total_w


# ---------------------------------------------------------------------------
# Legacy helpers — signatures preserved for downstream compatibility
# ---------------------------------------------------------------------------

def fit_half_equivalent(pace_medium, pace_long):
    """Estimate half-marathon equivalent time from training run paces."""
    if pace_medium and pace_long:
        return (0.55 * pace_long + 0.45 * pace_medium) * 21.1
    if pace_long:
        return pace_long * 21.1
    if pace_medium:
        return pace_medium * 21.1
    return None


def riegel_projection(time_seconds, distance_km, target_km=42.195, exponent=1.06):
    """T2 = T1 × (D2/D1)^exponent.  Default exponent 1.06 (Riegel, 1977)."""
    if not time_seconds or distance_km <= 0:
        return None
    return time_seconds * ((target_km / distance_km) ** exponent)


def vo2max_estimate_from_runs(runs):
    """Estimate VO2max from the fastest qualifying training runs (5–21.5 km)."""
    candidates = []
    for run in runs:
        distance = float(run.get("distance_km") or 0.0)
        moving = float(run.get("moving_time_sec") or 0.0)
        if distance < 5 or distance > 21.5 or moving <= 0:
            continue
        velocity_kmh = distance / (moving / 3600.0)
        candidates.append(3.5 + (velocity_kmh * 3.77))
    return round(max(candidates), 1) if candidates else None


def vo2max_marathon_projection(vo2max):
    """Project marathon time from a VO2max estimate."""
    if not vo2max or vo2max <= 3.5:
        return None
    marathon_velocity = (vo2max - 3.5) / 3.77
    if marathon_velocity <= 0:
        return None
    return (42.195 / marathon_velocity) * 3600.0


# ---------------------------------------------------------------------------
# Primary public API
# ---------------------------------------------------------------------------

def marathon_prediction_seconds(metrics) -> float | None:
    """Predict marathon finishing time in seconds.

    Blends VDOT (from recent races) with training-run Riegel estimates,
    then applies:
      1. Wall risk factor  — if HM→FM VDOT gap data is available
      2. TSB adjustment    — from load_engine (freshness/fatigue)
      3. Rebuild penalty   — if runner is returning from a gap

    The wall factor penalises the last 10.195 km based on the runner's
    historical HM→FM VDOT gap:
        high (gap > 8):    km-32 ×1.14  → overall ×1.034
        medium (gap 4–8):  km-32 ×1.10  → overall ×1.024
        low   (gap < 4):   km-32 ×1.03  → overall ×1.007
    """
    today = _date.today()

    best_vdot, days_since_last_race = _best_vdot_from_metrics(metrics, today)
    vdot_marathon = vdot_to_race_time_seconds(best_vdot, _FM_M) if best_vdot else None

    training_est = _training_run_estimate(metrics)

    if not training_est:
        pace_long = metrics.get("pace_long")
        pace_medium = metrics.get("pace_medium")
        if pace_long:
            training_est = pace_long * 42.195 * 1.10
        elif pace_medium:
            training_est = pace_medium * 42.195 * 1.14

    if vdot_marathon and training_est:
        if days_since_last_race is not None and days_since_last_race < 60:
            marathon_time = 0.70 * vdot_marathon + 0.30 * training_est
        else:
            marathon_time = 0.40 * vdot_marathon + 0.60 * training_est
    elif vdot_marathon:
        marathon_time = vdot_marathon
    elif training_est:
        marathon_time = training_est
    else:
        return None

    # --- Personal-best floor ---
    # If the training-data estimate is >20% slower than the stored FM PB the
    # Strava data is stale or sparse.  Anchor to PB VDOT instead so the
    # prediction never regresses badly below demonstrated fitness.
    fm_pb_sec = float(metrics.get("personal_best_fm_seconds") or 0.0) or None
    if fm_pb_sec and marathon_time > fm_pb_sec * 1.20:
        pb_vdot = vdot_from_race(_FM_M, fm_pb_sec / 60.0)
        if pb_vdot:
            anchored = vdot_to_race_time_seconds(pb_vdot, _FM_M)
            if anchored:
                marathon_time = anchored

    # --- Wall risk adjustment ---
    if fm_pb_sec:
        fm_vdot = vdot_from_race(42195.0, fm_pb_sec / 60.0)
        by_dist = _vdot_by_distance(metrics, today)
        hm_vdot = by_dist.get("hm") or by_dist.get("10k")
        wall = compute_wall_risk(hm_vdot, fm_vdot)
    else:
        wall = {"risk": "unknown"}

    marathon_time *= _WALL_OVERALL_FACTOR[wall["risk"]]

    # --- TSB adjustment ---
    tsb = float(metrics.get("tsb_proxy") or 0.0)
    if tsb > 5:
        marathon_time *= 0.985
    elif tsb < -10:
        marathon_time *= 1.020

    # --- Rebuild penalty ---
    if metrics.get("rebuild_mode"):
        marathon_time *= 1.05

    return marathon_time


def predict_all_distances(metrics, today=None) -> dict | None:
    """Predict race times for 5 K, 10 K, half marathon, and full marathon.

    Returns:
        {
            "5k":            seconds (int) or None,
            "10k":           seconds (int) or None,
            "half_marathon": seconds (int) or None,
            "marathon":      seconds (int),
            "confidence":    "high" | "medium" | "low",
        }
    or None if insufficient data.
    """
    if today is None:
        today = _date.today()

    best_vdot, days_since_last_race = _best_vdot_from_metrics(metrics, today)
    marathon_secs = marathon_prediction_seconds(metrics)
    if not marathon_secs:
        return None

    # Derive PB VDOT from stored personal best — used as anchor when race
    # data is absent or shows lower fitness than demonstrated capability.
    fm_pb_sec = float(metrics.get("personal_best_fm_seconds") or 0.0) or None
    pb_vdot = vdot_from_race(_FM_M, fm_pb_sec / 60.0) if fm_pb_sec else None

    # Use the best available VDOT: race-labelled effort > PB > nothing.
    # Taking max() ensures a slow stale race never drags predictions below
    # the athlete's demonstrated fitness ceiling.
    if best_vdot and pb_vdot:
        anchor_vdot = max(best_vdot, pb_vdot)
    elif best_vdot:
        anchor_vdot = best_vdot
    elif pb_vdot:
        anchor_vdot = pb_vdot
    else:
        anchor_vdot = None

    if anchor_vdot:
        five_k = vdot_to_race_time_seconds(anchor_vdot, _5K_M)
        ten_k  = vdot_to_race_time_seconds(anchor_vdot, _10K_M)
        half_m = vdot_to_race_time_seconds(anchor_vdot, _HM_M)
    else:
        five_k = riegel_projection(marathon_secs, 42.195, 5.0,      1.06)
        ten_k  = riegel_projection(marathon_secs, 42.195, 10.0,     1.06)
        half_m = riegel_projection(marathon_secs, 42.195, 21.0975,  1.06)

    race_count = len(metrics.get("recent_race_runs", []))
    long_count = len(metrics.get("long_runs", []))
    sim_count  = len(metrics.get("race_simulation_runs", []))

    if days_since_last_race is not None and days_since_last_race < 60 and race_count >= 1:
        confidence = "high"
    elif (
        (days_since_last_race is not None and days_since_last_race < 180)
        or (long_count >= 3 and sim_count >= 1)
    ):
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "5k":            round(five_k)  if five_k  else None,
        "10k":           round(ten_k)   if ten_k   else None,
        "half_marathon": round(half_m)  if half_m  else None,
        "marathon":      round(marathon_secs),
        "confidence":    confidence,
    }
