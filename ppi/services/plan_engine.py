from datetime import date, datetime, timedelta


LONG_RUN_LADDER = [21.0, 24.0, 28.0, 30.0, 32.0]


def goal_marathon_pace(weekly_goal):
    return float(weekly_goal.get("goal_marathon_pace_sec_per_km") or 0.0)


def _completed_long_run_step(longest_km):
    completed = 0.0
    for step in LONG_RUN_LADDER:
        if longest_km >= step:
            completed = step
        else:
            break
    return completed


def _next_long_run_target(phase, longest_km, next_milestone, weekly_target, weekly_goal, apply_capacity_cap=True):
    weeks_to_race = float(weekly_goal.get("weeks_to_race") or 0.0)
    completed = _completed_long_run_step(longest_km)
    race_distance_km = float(weekly_goal.get("race_distance_km") or 42.195)
    practical_peak = min(32.0, max(24.0, round(race_distance_km * 0.76)))
    allowed_steps = [step for step in LONG_RUN_LADDER if step <= practical_peak]
    if not allowed_steps:
        allowed_steps = [16.0]

    base_target = next((step for step in allowed_steps if step > completed), allowed_steps[-1])
    hinted_target = min(practical_peak, float(next_milestone or base_target))
    progression_target = max(base_target, hinted_target if hinted_target in allowed_steps else base_target)
    capacity_cap = round(weekly_target * 0.35, 1)

    if phase == "rebuild" and apply_capacity_cap:
        return max(14.0, min(18.0, capacity_cap))
    if phase == "recovery" and apply_capacity_cap:
        # Cutback based on 78% of longest_km — not capped by weekly volume so the
        # long run is always a meaningful effort (e.g. 21 km) rather than a tiny
        # number driven by a low weekly_target * 0.30 cap.
        # When apply_capacity_cap=False (template call), fall through to normal
        # progression so Sunday always advances to the next ladder milestone.
        return max(14.0, min(max(16.0, round(longest_km * 0.78, 1)), practical_peak))
    if phase == "taper":
        if weeks_to_race <= 1:
            return 0.0
        if weeks_to_race <= 2:
            return max(8.0, min(14.0, round(longest_km * 0.45, 1)))
        return max(14.0, min(20.0, round(longest_km * 0.70, 1)))

    if phase == "base":
        progression_target = min(progression_target, 24.0)
    elif phase == "build":
        progression_target = min(max(progression_target, 18.0), 28.0)
    elif phase == "peak":
        if weeks_to_race > 8:
            progression_target = min(progression_target, 28.0)
        else:
            progression_target = min(max(progression_target, 28.0 if completed >= 24.0 else progression_target), practical_peak)

    if phase in {"base", "build", "peak"} and longest_km >= 16.0:
        progression_target = max(progression_target, min(practical_peak, round(longest_km, 1)))

    if not apply_capacity_cap or capacity_cap <= 0:
        return progression_target
    return max(min(progression_target, capacity_cap), min(capacity_cap, max(16.0, completed)))


def _goal_seconds(weekly_goal):
    pace_sec_per_km = float(weekly_goal.get("goal_marathon_pace_sec_per_km") or 0.0)
    race_distance_km = float(weekly_goal.get("race_distance_km") or 42.195)
    if pace_sec_per_km > 0 and race_distance_km > 0:
        return pace_sec_per_km * race_distance_km
    return 4 * 3600.0


def _goal_band(weekly_goal):
    goal_seconds = _goal_seconds(weekly_goal)
    if goal_seconds < 3 * 3600 + 15 * 60:
        return "advanced"
    if goal_seconds < 3 * 3600 + 45 * 60:
        return "performance"
    if goal_seconds < 4 * 3600 + 15 * 60:
        return "sub4"
    if goal_seconds <= 5 * 3600:
        return "completion_plus"
    return "finish"


def _goal_band_phase_floor(goal_band, phase):
    floors = {
        "advanced": {"base": 60.0, "build": 72.0, "peak": 84.0, "recovery": 52.0, "taper": 38.0, "rebuild": 26.0},
        "performance": {"base": 48.0, "build": 58.0, "peak": 68.0, "recovery": 42.0, "taper": 32.0, "rebuild": 24.0},
        "sub4": {"base": 38.0, "build": 46.0, "peak": 54.0, "recovery": 34.0, "taper": 26.0, "rebuild": 22.0},
        "completion_plus": {"base": 32.0, "build": 38.0, "peak": 44.0, "recovery": 28.0, "taper": 22.0, "rebuild": 20.0},
        "finish": {"base": 26.0, "build": 32.0, "peak": 38.0, "recovery": 24.0, "taper": 20.0, "rebuild": 18.0},
    }
    return floors.get(goal_band, floors["sub4"]).get(phase, 38.0)


def _long_run_support_share(weekly_goal):
    phase = weekly_goal.get("phase", "build")
    goal_band = _goal_band(weekly_goal)
    consistency = float(weekly_goal.get("training_consistency_ratio") or 0.0)
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    atl_spike = bool(weekly_goal.get("atl_spike"))

    if rebuild_mode or high_fatigue or atl_spike:
        return 0.35
    if phase == "taper":
        return 0.45
    if phase == "recovery":
        return 0.38

    by_band = {
        "advanced": {"base": 0.35, "build": 0.35, "peak": 0.36},
        "performance": {"base": 0.36, "build": 0.37, "peak": 0.38},
        "sub4": {"base": 0.38, "build": 0.39, "peak": 0.40},
        "completion_plus": {"base": 0.40, "build": 0.41, "peak": 0.43},
        "finish": {"base": 0.42, "build": 0.44, "peak": 0.46},
    }
    share = by_band.get(goal_band, by_band["sub4"]).get(phase, 0.38)
    if consistency >= 0.8 and phase in {"build", "peak"}:
        share += 0.01
    return min(0.46, share)


def _recent_weekly_anchor(weekly_goal, baseline_weekly_target):
    prior_avg = float(weekly_goal.get("prior_avg_km") or 0.0)
    recent_avg = float(weekly_goal.get("recent_avg_km") or 0.0)
    if prior_avg > 0 and recent_avg > 0:
        return round((prior_avg * 0.7) + (recent_avg * 0.3), 1)
    if prior_avg > 0:
        return round(prior_avg, 1)
    if recent_avg > 0:
        return round(recent_avg, 1)
    return round(max(20.0, baseline_weekly_target * 0.75), 1)


def _safe_weekly_cap(recent_anchor, weekly_goal):
    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    moderate_fatigue = bool(weekly_goal.get("moderate_fatigue"))
    atl_spike = bool(weekly_goal.get("atl_spike"))
    consistency = float(weekly_goal.get("training_consistency_ratio") or 0.0)

    if rebuild_mode:
        ramp_pct = 0.03
    elif high_fatigue or atl_spike:
        ramp_pct = 0.00
    elif moderate_fatigue:
        ramp_pct = 0.05
    elif consistency >= 0.75 and phase in {"build", "peak"}:
        ramp_pct = 0.12
    else:
        ramp_pct = 0.10
    return round(recent_anchor * (1.0 + ramp_pct), 1)


def _controlled_progression_cap(recent_anchor, weekly_goal):
    phase = weekly_goal.get("phase", "build")
    consistency = float(weekly_goal.get("training_consistency_ratio") or 0.0)
    longest_km = float(weekly_goal.get("current_longest_km") or 0.0)
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    atl_spike = bool(weekly_goal.get("atl_spike"))

    if rebuild_mode or high_fatigue or atl_spike:
        step_km = 2.0
    elif phase == "base":
        step_km = 4.0
    elif phase == "build":
        step_km = 5.0
    elif phase == "peak":
        step_km = 6.0
    else:
        step_km = 3.0

    if longest_km >= 18.0:
        step_km += 2.0
    elif longest_km >= 14.0:
        step_km += 1.0

    if consistency >= 0.75:
        step_km += 1.0
    return round(recent_anchor + step_km, 1)


def _calibrated_weekly_target(weekly_goal, baseline_weekly_target, long_target):
    phase = weekly_goal.get("phase", "build")
    goal_band = _goal_band(weekly_goal)
    support_share = _long_run_support_share(weekly_goal)
    recent_anchor = _recent_weekly_anchor(weekly_goal, baseline_weekly_target)
    goal_floor = _goal_band_phase_floor(goal_band, phase)
    safe_cap = _safe_weekly_cap(recent_anchor, weekly_goal)
    controlled_cap = _controlled_progression_cap(recent_anchor, weekly_goal)
    allowed_growth_cap = max(safe_cap, controlled_cap)
    baseline_soft_target = min(max(baseline_weekly_target, recent_anchor), allowed_growth_cap)
    floor_progression_target = min(goal_floor, allowed_growth_cap)

    longest_km = float(weekly_goal.get("current_longest_km") or 0.0)
    consistency = float(weekly_goal.get("training_consistency_ratio") or 0.0)
    established = consistency >= 0.65 or recent_anchor >= goal_floor * 0.75
    durable_long_base = longest_km >= 18.0
    current_long_support = round(longest_km / max(0.3, support_share), 1) if longest_km >= 16.0 else 0.0
    next_long_support = round(long_target / max(0.3, support_share), 1) if long_target > 0 else 0.0

    weekly_target = max(recent_anchor, baseline_soft_target, floor_progression_target)
    if (current_long_support > 0 and baseline_weekly_target >= current_long_support) or (
        next_long_support > 0 and baseline_weekly_target >= next_long_support
    ):
        weekly_target = max(weekly_target, baseline_weekly_target)
    if established and current_long_support > 0:
        weekly_target = max(weekly_target, current_long_support)
    elif durable_long_base and current_long_support > 0:
        support_bridge = max(goal_floor + 6.0, recent_anchor + 12.0)
        weekly_target = max(weekly_target, min(current_long_support, support_bridge))
    elif next_long_support > weekly_target:
        support_bridge = recent_anchor + (6.0 if phase in {"base", "build"} else 8.0)
        weekly_target = max(weekly_target, min(next_long_support, support_bridge))

    if phase in {"taper", "recovery", "rebuild"}:
        weekly_target = min(weekly_target, max(baseline_weekly_target, allowed_growth_cap))
    return round(max(18.0, weekly_target), 1)


def _calibrated_long_run_target(weekly_goal, longest_km, progression_target, weekly_target):
    phase = weekly_goal.get("phase", "build")
    support_cap = round(max(0.0, weekly_target * _long_run_support_share(weekly_goal)), 1)
    if phase in {"taper", "recovery", "rebuild"}:
        return round(min(progression_target, support_cap if support_cap > 0 else progression_target), 1)
    if support_cap <= 0:
        return round(progression_target, 1)

    long_target = min(progression_target, support_cap)
    if longest_km >= 16.0 and support_cap >= longest_km:
        long_target = max(long_target, round(longest_km, 1))
    return round(max(12.0, long_target), 1)


def plan_meta_for_session(session_name):
    catalog = {
        "Race Day": {"intensity": "race", "importance": "High", "purpose": "Execute the marathon race plan with controlled pacing and fueling."},
        "Long Run": {"intensity": "long_run", "importance": "High", "purpose": "Build marathon endurance and fueling durability."},
        "Medium Long Run": {"intensity": "steady_long", "importance": "Medium", "purpose": "Extend aerobic endurance and improve long-run durability without full long-run stress."},
        "Tempo Run": {"intensity": "tempo", "importance": "High", "purpose": "Improve marathon-specific strength and threshold control."},
        "Speed Session": {"intensity": "speed", "importance": "High", "purpose": "Develop economy, leg speed, and top-end aerobic power."},
        "Marathon Pace Run": {"intensity": "marathon_specific", "importance": "High", "purpose": "Practice goal-race rhythm and marathon-specific durability."},
        "Aerobic Run": {"intensity": "aerobic", "importance": "Medium", "purpose": "Build aerobic endurance and support weekly mileage."},
        "Steady Run": {"intensity": "steady", "importance": "Medium", "purpose": "Build aerobic strength without excessive fatigue."},
        "Easy Run": {"intensity": "easy", "importance": "Low", "purpose": "Absorb prior load and keep volume consistent."},
        "Recovery Run": {"intensity": "recovery", "importance": "Low", "purpose": "Reduce fatigue and keep the week moving without strain."},
        "Strength": {"intensity": "strength", "importance": "Medium", "purpose": "Maintain durability and injury resistance."},
    }
    return catalog.get(session_name, {"intensity": "easy", "importance": "Low", "purpose": "Support the weekly training cycle."})


def quality_session_prescription(session_name, target_km, weekly_goal):
    target_km = int(round(float(target_km or 0.0)))
    if target_km <= 0:
        return None

    phase = str(weekly_goal.get("phase") or "build").lower()
    week_type = str(weekly_goal.get("progression_week_type") or "").lower()
    cutback_like = phase in {"recovery", "rebuild"} or week_type in {"cutback", "recovery", "rebuild"}
    meta = plan_meta_for_session(session_name)

    if cutback_like or session_name not in {"Steady Run", "Tempo Run", "Marathon Pace Run", "Speed Session", "Aerobic Run"}:
        return {
            "structure_summary": f"{target_km} km aerobic running.",
            "pace_guidance": meta.get("purpose") or "Keep the effort controlled.",
            "purpose": meta.get("purpose"),
        }

    if session_name == "Aerobic Run":
        return {
            "structure_summary": f"{target_km} km continuous aerobic running.",
            "pace_guidance": "Comfortable aerobic effort throughout.",
            "purpose": meta.get("purpose"),
        }

    if session_name == "Speed Session":
        warm_km = 2
        cool_km = 2 if target_km >= 8 else 1
        rep_count = max(4, min(8, target_km - warm_km - cool_km + 1))
        return {
            "structure_summary": f"{warm_km} km easy + {rep_count} x 1 min quick / 1 min easy + {cool_km} km easy",
            "pace_guidance": "Run the quick reps fast but controlled, then recover fully between reps.",
            "purpose": meta.get("purpose"),
        }

    if session_name == "Steady Run":
        warm_km = 2
        cool_km = 1
        steady_km = max(3, target_km - warm_km - cool_km)
        return {
            "structure_summary": f"{warm_km} km easy + {steady_km} km steady + {cool_km} km easy",
            "pace_guidance": "Settle into a controlled steady effort, stronger than easy but below tempo.",
            "purpose": meta.get("purpose"),
        }

    if session_name == "Tempo Run":
        warm_km = 2
        cool_km = 1
        tempo_km = max(3, target_km - warm_km - cool_km)
        return {
            "structure_summary": f"{warm_km} km easy + {tempo_km} km tempo + {cool_km} km easy",
            "pace_guidance": "Run the tempo block at a comfortably hard effort that stays controlled.",
            "purpose": meta.get("purpose"),
        }

    if session_name == "Marathon Pace Run":
        if target_km <= 7:
            warm_km, mp_km, cool_km = 2, max(3, target_km - 3), 1
        elif target_km <= 9:
            warm_km, mp_km, cool_km = 2, target_km - 3, 1
        else:
            warm_km, mp_km, cool_km = 2, target_km - 4, 2
        return {
            "structure_summary": f"{warm_km} km easy + {mp_km} km at MP + {cool_km} km easy",
            "pace_guidance": "Lock into goal marathon pace calmly and keep the rhythm controlled.",
            "purpose": meta.get("purpose"),
        }

    return {
        "structure_summary": f"{target_km} km quality running.",
        "pace_guidance": meta.get("purpose") or "Keep the work controlled.",
        "purpose": meta.get("purpose"),
    }


def classify_run_completion(actual_km, target_km):
    if not target_km or target_km <= 0:
        return "completed", 100, round(max(0.0, actual_km or 0.0), 1)
    if actual_km is None or actual_km <= 0:
        return "missed", 0, 0.0

    raw_pct = int(round((actual_km / target_km) * 100))
    completion_pct = min(100, raw_pct)
    extra_km = round(max(0.0, actual_km - target_km), 1)
    if actual_km >= 0.9 * target_km:
        return "completed", completion_pct, extra_km
    if actual_km >= 0.5 * target_km:
        return "partial", completion_pct, extra_km
    return "missed", completion_pct, extra_km


def classify_quality_completion(session_name, actual_km, target_km, pace_sec_per_km, weekly_goal):
    status, completion_pct, extra_km = classify_run_completion(actual_km, target_km)
    marathon_pace = goal_marathon_pace(weekly_goal)
    if not pace_sec_per_km or marathon_pace <= 0:
        return status, completion_pct, extra_km

    if session_name == "Marathon Pace Run":
        pace_ok = abs(pace_sec_per_km - marathon_pace) / marathon_pace <= 0.05
        if actual_km >= 0.9 * target_km and pace_ok:
            return "completed", completion_pct, extra_km
        if actual_km >= 0.6 * target_km and abs(pace_sec_per_km - marathon_pace) / marathon_pace <= 0.08:
            return "partial", completion_pct, extra_km
        return "missed", completion_pct, extra_km

    if session_name == "Speed Session":
        if pace_sec_per_km <= marathon_pace * 0.95 and actual_km >= 0.85 * target_km:
            return "completed", completion_pct, extra_km
        if pace_sec_per_km <= marathon_pace and actual_km >= 0.6 * target_km:
            return "partial", completion_pct, extra_km
        return "missed", completion_pct, extra_km

    if session_name == "Steady Run":
        if pace_sec_per_km <= marathon_pace * 1.10 and actual_km >= 0.9 * target_km:
            return "completed", completion_pct, extra_km
        if actual_km >= 0.6 * target_km:
            return "partial", completion_pct, extra_km
        return "missed", completion_pct, extra_km

    if session_name == "Tempo Run":
        if pace_sec_per_km <= marathon_pace * 0.98 and actual_km >= 0.85 * target_km:
            return "completed", completion_pct, extra_km
        if pace_sec_per_km <= marathon_pace * 1.03 and actual_km >= 0.6 * target_km:
            return "partial", completion_pct, extra_km
        return "missed", completion_pct, extra_km

    return status, completion_pct, extra_km


def select_best_run_for_session(run_acts, session_name, weekly_goal, run_pace_fn):
    marathon_pace = goal_marathon_pace(weekly_goal)
    if not run_acts:
        return None
    if session_name == "Race Day":
        return max(run_acts, key=lambda a: (float(a.distance_km or 0.0), -(run_pace_fn(a) or 9999)))
    if session_name == "Long Run":
        return max(run_acts, key=lambda a: (float(a.distance_km or 0.0), -(run_pace_fn(a) or 9999)))
    if session_name == "Marathon Pace Run":
        return min(
            run_acts,
            key=lambda a: (
                abs((run_pace_fn(a) or marathon_pace or 9999) - marathon_pace) if marathon_pace else (run_pace_fn(a) or 9999),
                -float(a.distance_km or 0.0),
            ),
        )
    if session_name in {"Speed Session", "Tempo Run", "Steady Run"}:
        return min(run_acts, key=lambda a: ((run_pace_fn(a) or 9999), -float(a.distance_km or 0.0)))
    return None


def build_weekly_plan_template(weekly_goal, long_run):
    """Build a fixed 7-day training template with a coherent weekly contract.

    The planner must satisfy two rules:
    1. The sum of planned run rows equals the chosen weekly target.
    2. If the next long-run step requires a larger week under the 35% rule,
       the weekly target is raised explicitly to support it.
    """
    explicit_weekly_target = float(weekly_goal.get("weekly_goal_km") or 0.0)
    if explicit_weekly_target > 0:
        baseline_weekly_target = explicit_weekly_target
    else:
        ctl = float(weekly_goal.get("ctl_proxy") or 0.0)
        if ctl < 30:
            baseline_weekly_target = 45.0
        elif ctl < 45:
            baseline_weekly_target = 55.0
        elif ctl < 60:
            baseline_weekly_target = 65.0
        else:
            baseline_weekly_target = 75.0

    phase = str(weekly_goal.get("phase", "build")).lower()
    progression_week_type = str(weekly_goal.get("progression_week_type") or "").lower()
    template_phase = "recovery" if progression_week_type in {"cutback", "recovery"} and phase not in {"taper", "rebuild"} else phase
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    longest_km = float(long_run.get("longest_km") or 0.0)
    next_milestone = float(long_run.get("next_milestone_km") or max(22.0, min(32.0, longest_km + 2.0)))
    weekly_goal = {**weekly_goal, "current_longest_km": longest_km, "phase": template_phase}

    # Long run target from progression ladder — not capacity-capped.
    # The CTL-based weekly_target drives easy/tempo volume only; the long
    # run is determined purely by the progression ladder so it always
    # advances to the next milestone regardless of total weekly km.
    progression_long_target = _next_long_run_target(
        "rebuild" if rebuild_mode else template_phase,
        longest_km,
        next_milestone,
        baseline_weekly_target,
        weekly_goal,
        apply_capacity_cap=False,
    )
    weekly_target = _calibrated_weekly_target(weekly_goal, baseline_weekly_target, progression_long_target)
    long_target = _calibrated_long_run_target(weekly_goal, longest_km, progression_long_target, weekly_target)
    non_long_total = round(max(0.0, weekly_target - long_target), 1)

    sessions, run_slots, weights, min_km = _weekly_session_structure(weekly_goal, weekly_target, long_target)
    long_day_index = next((idx for idx, session in enumerate(sessions) if session == "Long Run"), 6)

    remaining_total = non_long_total
    allocated = []
    remaining_weight = sum(weights)
    for idx, (weight, floor_km) in enumerate(zip(weights, min_km)):
        slots_left = len(weights) - idx
        if slots_left == 1:
            km = round(max(floor_km, remaining_total), 1)
        else:
            target_share = remaining_total * (weight / remaining_weight) if remaining_weight > 0 else 0.0
            max_allowing_remainder = remaining_total - sum(min_km[idx + 1 :])
            km = round(max(floor_km, min(target_share, max_allowing_remainder)), 1)
        allocated.append(km)
        remaining_total = round(max(0.0, remaining_total - km), 1)
        remaining_weight -= weight

    if allocated:
        drift = round(non_long_total - sum(allocated), 1)
        allocated[-1] = round(max(min_km[-1], allocated[-1] + drift), 1)

    template = {}
    allocated_map = dict(zip(run_slots, allocated))
    for day_index in range(7):
        session = sessions[day_index]
        if session == "Strength":
            template[day_index] = {"workout_type": "STRENGTH", "session": session, "target_km": None, **plan_meta_for_session(session)}
        elif session == "Rest":
            template[day_index] = {"workout_type": "REST", "session": "Rest", "target_km": None, **plan_meta_for_session("Easy Run")}
        else:
            target_km = long_target if day_index == long_day_index else allocated_map.get(day_index, 0.0)
            template[day_index] = {"workout_type": "RUN", "session": session, "target_km": round(target_km, 1), **plan_meta_for_session(session)}
    return apply_race_week_overrides(template, weekly_goal)


def apply_race_week_overrides(template, weekly_goal):
    race_date = weekly_goal.get("race_date")
    week_start = weekly_goal.get("week_start")
    race_distance_km = float(weekly_goal.get("race_distance_km") or 42.195)
    if isinstance(race_date, str):
        race_date = date.fromisoformat(race_date)
    if isinstance(week_start, str):
        week_start = date.fromisoformat(week_start)
    if not race_date or not week_start:
        return template
    race_offset = (race_date - week_start).days
    if race_offset < 0 or race_offset > 6:
        return template

    for idx in range(7):
        if idx > race_offset:
            template[idx] = {"workout_type": "REST", "session": "Rest", "target_km": None, "intensity": "rest", "importance": "Low", "purpose": "Post-race recovery."}
            continue
        if idx == race_offset:
            template[idx] = {"workout_type": "RUN", "session": "Race Day", "target_km": race_distance_km, **plan_meta_for_session("Race Day")}
        elif idx == race_offset - 1:
            template[idx] = {"workout_type": "RUN", "session": "Recovery Run", "target_km": 4.0, **plan_meta_for_session("Recovery Run")}
        elif idx == race_offset - 2:
            template[idx] = {"workout_type": "RUN", "session": "Marathon Pace Run", "target_km": 6.0, **plan_meta_for_session("Marathon Pace Run")}
        elif idx == race_offset - 3:
            template[idx] = {"workout_type": "RUN", "session": "Easy Run", "target_km": 5.0, **plan_meta_for_session("Easy Run")}
        elif idx == race_offset - 4:
            template[idx] = {"workout_type": "STRENGTH", "session": "Strength", "target_km": None, **plan_meta_for_session("Strength")}
        elif idx == race_offset - 5:
            template[idx] = {"workout_type": "RUN", "session": "Recovery Run", "target_km": 5.0, **plan_meta_for_session("Recovery Run")}
        elif idx == race_offset - 6:
            template[idx] = {"workout_type": "RUN", "session": "Easy Run", "target_km": 6.0, **plan_meta_for_session("Easy Run")}
    return template


def apply_adaptive_plan(plan_items, today_local, weekly_goal):
    weekly_goal_km = float(weekly_goal.get("weekly_goal_km") or 0.0)
    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    max_safe_run = float(weekly_goal.get("max_safe_run") or max(10.0, weekly_goal_km * 0.35))
    long_run_failed_recent = bool(weekly_goal.get("long_run_failed_recent"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    moderate_fatigue = bool(weekly_goal.get("moderate_fatigue"))
    atl_spike = bool(weekly_goal.get("atl_spike"))
    allow_progression = bool(weekly_goal.get("allow_progression"))
    missed_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and item["status"] in {"missed", "different_activity", "skipped"}]
    partial_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and item["status"] == "partial"]
    overperformed_runs = [
        item for item in plan_items
        if item["workout_type"] == "RUN"
        and item["date"] < today_local
        and (item["status"] == "overdone" or (item.get("extra_km") or 0.0) >= 1.0)
    ]
    completed_run_km = sum(item["actual_km"] or 0.0 for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"])
    future_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] >= today_local and item["status"] in {"planned", "today"}]
    if not future_runs:
        return plan_items

    if rebuild_mode:
        for item in future_runs:
            if item["session"] in {"Tempo Run", "Speed Session", "Marathon Pace Run"}:
                item["session"] = "Aerobic Run"
                item["adaptive_note"] = "Quality reduced while rebuilding consistency after a gap."
            if item["session"] == "Long Run":
                item["planned_km"] = round(min(item["planned_km"] or 0.0, max_safe_run, 18.0), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Long run capped while rebuilding durability."
            item.update(plan_meta_for_session(item["session"]))

    if phase == "recovery":
        for item in future_runs:
            if item["session"] in {"Tempo Run", "Speed Session", "Marathon Pace Run"}:
                item["session"] = "Aerobic Run"
                item["planned_km"] = round(max(6.0, (item["planned_km"] or 0.0) * 0.8), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery week reduces workout intensity while preserving consistency."
                item.update(plan_meta_for_session(item["session"]))

    if len(missed_runs) + len(partial_runs) >= 2:
        for item in future_runs:
            if item["session"] in {"Tempo Run", "Speed Session", "Marathon Pace Run"}:
                item["session"] = "Aerobic Run"
                item["planned_km"] = round(max(6.0, (item["planned_km"] or 0.0) * 0.85), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Quality session reduced after missed work earlier in the week."
                item.update(plan_meta_for_session(item["session"]))
                break

    if high_fatigue or moderate_fatigue:
        for item in future_runs:
            if item["session"] in {"Easy Run", "Aerobic Run", "Steady Run"}:
                item["session"] = "Recovery Run"
                reduction = 0.75 if high_fatigue else 0.85
                item["planned_km"] = round(max(4.0, (item["planned_km"] or 0.0) * reduction), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery inserted to control fatigue."
                item.update(plan_meta_for_session(item["session"]))
                break
        for item in future_runs:
            if item["session"] == "Long Run":
                reduction = 0.9 if high_fatigue else 0.95
                item["planned_km"] = round(min(max_safe_run, max(12.0, (item["planned_km"] or 0.0) * reduction)), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                if not item.get("adaptive_note"):
                    item["adaptive_note"] = "Long run trimmed slightly to keep fatigue under control."

    if atl_spike:
        for item in future_runs:
            if item["session"] in {"Tempo Run", "Speed Session", "Marathon Pace Run", "Long Run"}:
                item["planned_km"] = round(max(6.0 if item["session"] != "Long Run" else 12.0, (item["planned_km"] or 0.0) * 0.9), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Load reduced because acute fatigue spiked."
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
            next_easy.update(plan_meta_for_session(next_easy["session"]))

    if allow_progression and phase not in {"taper", "rebuild", "recovery"} and not missed_runs and not partial_runs:
        next_key = next((item for item in future_runs if item["session"] in {"Long Run", "Marathon Pace Run", "Aerobic Run"}), None)
        if next_key:
            base_km = float(next_key["planned_km"] or 0.0)
            increase = 0.5 if next_key["session"] == "Long Run" else 0.3
            next_key["planned_km"] = round(
                min(
                    max_safe_run if next_key["session"] == "Long Run" else base_km + increase,
                    base_km + increase,
                ),
                1,
            )
            next_key["planned"] = f"{int(round(next_key['planned_km']))} km"
            next_key["adaptive_note"] = "Small progression allowed because fatigue is low and consistency is good."

    future_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] >= today_local and item["status"] in {"planned", "today"}]
    target_remaining = max(0.0, weekly_goal_km - completed_run_km)
    future_total = sum(item["planned_km"] or 0.0 for item in future_runs)
    if future_runs and future_total > 0:
        scale = min(1.05, max(0.85, target_remaining / future_total if target_remaining > 0 else 0.85))
        if phase == "taper":
            scale = min(scale, 1.0)
        if allow_progression:
            scale = min(scale, 1.03)
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
        item.update(plan_meta_for_session(item["session"]))
    return plan_items


def training_consistency_score(logs):
    planned_runs = [log for log in logs if getattr(log, "workout_type", None) == "RUN"]
    completed_runs = [log for log in planned_runs if getattr(log, "status", None) in {"completed", "moved", "overdone"}]
    if not planned_runs:
        return 0
    return int(round((len(completed_runs) / len(planned_runs)) * 100))


def _phase_for_weeks_to_race(weeks_to_race, rebuild_mode=False):
    weeks_to_race = float(weeks_to_race or 0.0)
    if rebuild_mode and weeks_to_race > 8.0:
        return "rebuild"
    if weeks_to_race <= 3.0:
        return "taper"
    if weeks_to_race <= 8.0:
        return "peak"
    if weeks_to_race <= 16.0:
        return "build"
    return "base"


def _progression_week_type(week_index, phase, weeks_to_race, rebuild_mode=False):
    if weeks_to_race <= 0:
        return "race"
    if phase == "taper":
        return "taper"
    if phase == "rebuild" or rebuild_mode:
        return "rebuild"
    if phase == "recovery":
        return "recovery"
    if week_index > 0 and (week_index + 1) % 4 == 0 and weeks_to_race > 4.0:
        return "cutback"
    return "build"


def _next_progression_long_milestone(longest_km):
    longest_km = float(longest_km or 0.0)
    for step in LONG_RUN_LADDER:
        if step > longest_km + 0.4:
            return step
    return LONG_RUN_LADDER[-1]


def _coerce_date(value):
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value[:10]).date()
        except Exception:
            return None
    return None


def effective_long_run_base_km(long_run, reference_date=None):
    reference_date = _coerce_date(reference_date) or date.today()
    long_run = dict(long_run or {})

    def _decayed(km_value, dt_value, *, current=False):
        km_value = float(km_value or 0.0)
        if km_value <= 0:
            return 0.0
        if current:
            return km_value
        dt_value = _coerce_date(dt_value)
        if not dt_value:
            return km_value * 0.75
        age_days = max(0, (reference_date - dt_value).days)
        if age_days <= 21:
            weight = 1.00
        elif age_days <= 42:
            weight = 0.90
        elif age_days <= 56:
            weight = 0.80
        elif age_days <= 84:
            weight = 0.65
        else:
            weight = 0.50
        return round(km_value * weight, 1)

    current_week_long = _decayed(long_run.get("current_week_long_km"), reference_date, current=True)
    latest_long = _decayed(long_run.get("latest_km"), long_run.get("latest_date"))
    longest_recent = _decayed(long_run.get("longest_km"), long_run.get("longest_date"))
    return round(max(current_week_long, latest_long, longest_recent), 1)


def prescribed_long_run_km(km_value, phase="build", race_distance_km=42.195):
    """Snap prescribed marathon long runs to coach-standard steps.

    Internal engine math can remain decimal-based, but the athlete-facing
    long-run prescription should usually land on clean marathon-plan values.
    """
    km_value = float(km_value or 0.0)
    phase = str(phase or "build").lower()
    if km_value <= 0:
        return 0

    if phase == "taper":
        taper_steps = [8, 10, 12, 14, 16, 18, 20]
        return min(taper_steps, key=lambda step: abs(step - km_value))
    if phase in {"recovery", "rebuild"}:
        easy_steps = [10, 12, 14, 16, 18, 20]
        return min(easy_steps, key=lambda step: abs(step - km_value))

    max_peak = 32 if float(race_distance_km or 42.195) >= 40 else 28
    standard_steps = [12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32]
    allowed_steps = [step for step in standard_steps if step <= max_peak]
    if km_value < 12:
        return int(round(km_value))
    return min(allowed_steps, key=lambda step: abs(step - km_value))


def _even_quality_block_km(long_run_km, ratio, minimum, maximum):
    long_run_km = float(long_run_km or 0.0)
    if long_run_km <= 0:
        return 0
    block = int(round(long_run_km * ratio))
    block = max(int(minimum), min(int(maximum), block))
    if block % 2 != 0:
        block += 1
    return max(0, min(block, max(0, int(round(long_run_km)) - 4)))


def _load_week_position(week_index):
    if week_index <= 0:
        return 0
    return week_index % 4


def _schedule_preferences(weekly_goal):
    training_days = int(weekly_goal.get("training_days_per_week") or 5)
    training_days = max(3, min(6, training_days))
    long_run_day = str(weekly_goal.get("long_run_day") or "sunday").lower()
    if long_run_day not in {"saturday", "sunday"}:
        long_run_day = "sunday"
    strength_days = int(weekly_goal.get("strength_days_per_week") or 2)
    strength_days = max(0, min(2, strength_days))
    return training_days, long_run_day, strength_days


def _assign_strength_days(sessions, strength_days, long_run_day):
    if strength_days <= 0:
        return sessions
    if long_run_day == "saturday":
        preferred = [2, 6, 4, 0]
    else:
        preferred = [2, 4, 0, 5]
    assigned = 0
    for idx in preferred:
        if assigned >= strength_days:
            break
        if sessions[idx] == "Rest":
            sessions[idx] = "Strength"
            assigned += 1
    return sessions


def _can_medium_long_run(weekly_goal, weekly_target, long_target, week_type):
    training_days, long_run_day, _strength_days = _schedule_preferences(weekly_goal)
    if week_type in {"cutback", "rebuild", "recovery", "taper"}:
        return False
    if training_days <= 3:
        return False
    if weekly_target < 42.0 or long_target < 20.0:
        return False
    if training_days == 4 and long_run_day == "saturday":
        return weekly_target >= 46.0 and long_target >= 20.0
    return True


def _quality_session_for_week(weekly_goal, weekly_target, can_medium_long):
    phase = str(weekly_goal.get("phase") or "build").lower()
    goal_band = _goal_band(weekly_goal)
    week_type = str(weekly_goal.get("progression_week_type") or ("build" if phase not in {"rebuild", "recovery", "taper"} else phase)).lower()
    week_index = int(weekly_goal.get("progression_week_index") or 0)
    weeks_to_race = float(weekly_goal.get("weeks_to_race") or 0.0)
    load_position = _load_week_position(week_index)
    strong_goal_band = goal_band in {"advanced", "performance", "sub4"}
    training_days, long_run_day, _strength_days = _schedule_preferences(weekly_goal)
    constrained = training_days <= 3
    dense_four_day_week = training_days == 4 and can_medium_long
    dense_six_day_week = training_days >= 6 and can_medium_long

    if phase in {"recovery", "rebuild"} or week_type in {"cutback", "recovery", "rebuild"}:
        return "Aerobic Run"

    if phase == "taper":
        return "Marathon Pace Run"

    if phase == "base":
        if strong_goal_band and weekly_target >= 42.0 and load_position == 1 and weeks_to_race <= 20.0:
            return "Steady Run"
        return "Aerobic Run"

    if phase == "build":
        if constrained:
            return "Steady Run" if weekly_target >= 40.0 else "Aerobic Run"
        if dense_four_day_week:
            if strong_goal_band and weekly_target >= 50.0:
                return "Marathon Pace Run" if load_position == 1 else "Steady Run"
            return "Steady Run" if weekly_target >= 40.0 else "Aerobic Run"
        if dense_six_day_week and strong_goal_band and weekly_target >= 50.0:
            return "Marathon Pace Run" if load_position == 1 else "Steady Run"
        if strong_goal_band and weekly_target >= 50.0:
            return "Tempo Run" if load_position in {0, 2} else "Marathon Pace Run"
        if strong_goal_band and weekly_target >= 44.0:
            return "Steady Run" if load_position == 0 else "Tempo Run"
        return "Steady Run" if weekly_target >= 40.0 else "Aerobic Run"

    if phase == "peak":
        if constrained or dense_four_day_week:
            return "Marathon Pace Run" if strong_goal_band and weekly_target >= 52.0 else "Steady Run"
        if dense_six_day_week:
            return "Marathon Pace Run" if strong_goal_band and weekly_target >= 54.0 else "Steady Run"
        if strong_goal_band and weekly_target >= 54.0:
            return "Tempo Run" if load_position in {0, 2} else "Marathon Pace Run"
        return "Steady Run"

    return "Tempo Run"


def _planned_week_layout(weekly_goal, quality_session, can_medium_long):
    training_days, long_run_day, strength_days = _schedule_preferences(weekly_goal)
    long_day = 5 if long_run_day == "saturday" else 6
    sessions = ["Rest"] * 7
    sessions[long_day] = "Long Run"

    phase = str(weekly_goal.get("phase") or "build").lower()
    week_type = str(weekly_goal.get("progression_week_type") or "").lower()
    constrained = training_days <= 3
    cutback_like = phase in {"recovery", "rebuild"} or week_type in {"cutback", "recovery", "rebuild"}

    if cutback_like:
        quality_session = "Aerobic Run"
        can_medium_long = False
    elif constrained and quality_session in {"Tempo Run", "Marathon Pace Run", "Speed Session"}:
        quality_session = "Steady Run" if phase in {"base", "build"} else "Tempo Run"

    if long_day == 6:
        if training_days >= 6:
            run_slots = [0, 1, 2, 3, 5, 6]
            role_sessions = [
                "Easy Run",
                quality_session,
                "Aerobic Run",
                "Medium Long Run" if can_medium_long else "Easy Run",
                "Recovery Run",
                "Long Run",
            ]
            role_weights = [0.14, 0.18, 0.16, 0.28 if can_medium_long else 0.22, 0.24 if can_medium_long else 0.30]
            role_min_km = [
                5.0,
                6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0,
                5.0,
                8.0 if can_medium_long else 6.0,
                4.0,
            ]
        elif training_days >= 5:
            run_slots = [0, 1, 3, 5, 6]
            role_sessions = ["Easy Run", quality_session, "Medium Long Run" if can_medium_long else "Easy Run", "Recovery Run", "Long Run"]
            role_weights = [0.18, 0.22, 0.36 if can_medium_long else 0.32, 0.24 if can_medium_long else 0.28]
            role_min_km = [6.0, 6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0, 8.0 if can_medium_long else 6.0, 5.0]
        elif training_days == 4:
            run_slots = [0, 1, 3, 6]
            role_sessions = ["Easy Run", quality_session, "Medium Long Run" if can_medium_long else "Easy Run", "Long Run"]
            role_weights = [0.28, 0.30, 0.42 if can_medium_long else 0.42]
            role_min_km = [6.0, 6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0, 8.0 if can_medium_long else 6.0]
        else:
            run_slots = [1, 3, 6]
            role_sessions = [quality_session, "Easy Run", "Long Run"]
            role_weights = [0.46, 0.54]
            role_min_km = [6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0, 6.0]
    else:
        if training_days >= 6:
            run_slots = [0, 1, 2, 3, 4, 5]
            role_sessions = [
                "Easy Run",
                quality_session,
                "Aerobic Run",
                "Medium Long Run" if can_medium_long else "Easy Run",
                "Recovery Run",
                "Long Run",
            ]
            role_weights = [0.14, 0.18, 0.16, 0.28 if can_medium_long else 0.22, 0.24 if can_medium_long else 0.30]
            role_min_km = [
                5.0,
                6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0,
                5.0,
                8.0 if can_medium_long else 6.0,
                4.0,
            ]
        elif training_days >= 5:
            run_slots = [0, 1, 3, 4, 5]
            role_sessions = ["Easy Run", quality_session, "Medium Long Run" if can_medium_long else "Easy Run", "Recovery Run", "Long Run"]
            role_weights = [0.18, 0.22, 0.36 if can_medium_long else 0.32, 0.24 if can_medium_long else 0.28]
            role_min_km = [6.0, 6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0, 8.0 if can_medium_long else 6.0, 5.0]
        elif training_days == 4:
            run_slots = [0, 1, 3, 5]
            role_sessions = ["Easy Run", quality_session, "Medium Long Run" if can_medium_long else "Easy Run", "Long Run"]
            role_weights = [0.28, 0.30, 0.42 if can_medium_long else 0.42]
            role_min_km = [6.0, 6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0, 8.0 if can_medium_long else 6.0]
        else:
            run_slots = [1, 3, 5]
            role_sessions = [quality_session, "Easy Run", "Long Run"]
            role_weights = [0.46, 0.54]
            role_min_km = [6.0 if quality_session in {"Tempo Run", "Marathon Pace Run"} else 5.0, 6.0]

    for idx, session_name in zip(run_slots, role_sessions):
        sessions[idx] = session_name

    sessions = _assign_strength_days(sessions, strength_days, long_run_day)
    non_long_run_slots = [idx for idx in run_slots if idx != long_day]
    return sessions, non_long_run_slots, role_weights, role_min_km


def _weekly_session_structure(weekly_goal, weekly_target, long_target):
    phase = str(weekly_goal.get("phase") or "build").lower()
    week_type = str(weekly_goal.get("progression_week_type") or ("build" if phase not in {"rebuild", "recovery", "taper"} else phase)).lower()
    can_medium_long = _can_medium_long_run(weekly_goal, weekly_target, long_target, week_type)
    quality_session = _quality_session_for_week(weekly_goal, weekly_target, can_medium_long)
    sessions, run_slots, weights, min_km = _planned_week_layout(weekly_goal, quality_session, can_medium_long)
    return sessions, run_slots, weights, min_km


def long_run_variant_for_week(phase, week_type, week_index, long_run_km, weekly_goal, previous_week_type=None):
    phase = str(phase or "build").lower()
    week_type = str(week_type or "build").lower()
    goal_band = _goal_band(weekly_goal)
    weeks_to_race = float(weekly_goal.get("weeks_to_race") or 0.0)
    long_run_km = prescribed_long_run_km(
        long_run_km,
        phase=phase,
        race_distance_km=float(weekly_goal.get("race_distance_km") or 42.195),
    )

    easy_variant = {
        "name": "Easy Long Run",
        "short_label": "Easy long run",
        "quality_block_km": 0,
        "quality_type": "easy",
        "pace_guidance": "Easy conversational pace throughout.",
        "note": "Keep the whole run relaxed and conversational.",
    }
    if week_type in {"cutback", "recovery", "rebuild"}:
        return {
            "name": "Cutback Long Run",
            "short_label": "Cutback long run",
            "quality_block_km": 0,
            "quality_type": "cutback",
            "pace_guidance": "Easy conversational pace throughout.",
            "note": "Shorter absorption week. Keep the effort easy all the way through.",
        }

    quality_eligible = goal_band in {"advanced", "performance", "sub4"} and long_run_km >= 20
    load_position = _load_week_position(week_index)
    if phase == "taper":
        if quality_eligible and weeks_to_race > 2.0 and long_run_km >= 18:
            block_km = _even_quality_block_km(long_run_km, ratio=0.28, minimum=4, maximum=6)
            return {
                "name": "Marathon Pace Long Run",
                "short_label": f"MP {block_km} km",
                "quality_block_km": block_km,
                "quality_type": "marathon_pace",
                "pace_guidance": f"{block_km} km at goal marathon pace inside the long run.",
                "note": f"Keep most of the run easy, with {block_km} km at goal marathon pace.",
            }
        return easy_variant

    if phase == "base":
        if quality_eligible and load_position == 1 and previous_week_type != "cutback" and long_run_km >= 20:
            block_km = _even_quality_block_km(long_run_km, ratio=0.24, minimum=4, maximum=6)
            return {
                "name": "Fast-Finish Long Run",
                "short_label": f"Fast finish {block_km} km",
                "quality_block_km": block_km,
                "quality_type": "fast_finish",
                "pace_guidance": f"Finish the last {block_km} km at steady to marathon effort.",
                "note": f"Run easy early, then finish the last {block_km} km strong but controlled.",
            }
        return easy_variant

    if phase == "build":
        if quality_eligible and load_position == 2 and previous_week_type != "cutback" and long_run_km >= 24:
            block_km = _even_quality_block_km(long_run_km, ratio=0.30, minimum=6, maximum=10)
            return {
                "name": "Marathon Pace Long Run",
                "short_label": f"MP {block_km} km",
                "quality_block_km": block_km,
                "quality_type": "marathon_pace",
                "pace_guidance": f"{block_km} km at goal marathon pace inside the long run.",
                "note": f"Keep the run easy around a {block_km} km block at goal marathon pace.",
            }
        if quality_eligible and load_position == 1 and long_run_km >= 20:
            block_km = _even_quality_block_km(long_run_km, ratio=0.25, minimum=4, maximum=8)
            return {
                "name": "Fast-Finish Long Run",
                "short_label": f"Fast finish {block_km} km",
                "quality_block_km": block_km,
                "quality_type": "fast_finish",
                "pace_guidance": f"Finish the final {block_km} km at steady to marathon effort.",
                "note": f"Run easy early, then finish the final {block_km} km stronger than easy pace.",
            }
        return easy_variant

    if phase == "peak":
        if quality_eligible and long_run_km >= 28:
            if load_position in {0, 2}:
                block_km = _even_quality_block_km(long_run_km, ratio=0.32, minimum=8, maximum=12)
                return {
                    "name": "Marathon Pace Long Run",
                    "short_label": f"MP {block_km} km",
                    "quality_block_km": block_km,
                    "quality_type": "marathon_pace",
                    "pace_guidance": f"{block_km} km at goal marathon pace inside the long run.",
                    "note": f"Structure this as an easy long run with {block_km} km at goal marathon pace.",
                }
            block_km = _even_quality_block_km(long_run_km, ratio=0.24, minimum=6, maximum=8)
            return {
                "name": "Fast-Finish Long Run",
                "short_label": f"Fast finish {block_km} km",
                "quality_block_km": block_km,
                "quality_type": "fast_finish",
                "pace_guidance": f"Finish the last {block_km} km at steady to marathon effort.",
                "note": f"Stay controlled early, then close the last {block_km} km with purpose.",
            }
        if quality_eligible and load_position == 1:
            block_km = _even_quality_block_km(long_run_km, ratio=0.24, minimum=4, maximum=6)
            return {
                "name": "Fast-Finish Long Run",
                "short_label": f"Fast finish {block_km} km",
                "quality_block_km": block_km,
                "quality_type": "fast_finish",
                "pace_guidance": f"Finish the last {block_km} km at steady to marathon effort.",
                "note": f"Keep the first part easy and lift the final {block_km} km.",
            }
        return easy_variant

    return easy_variant


def _projected_weekly_target_seed(previous_target, goal_band, phase, week_type, weekly_goal):
    previous_target = float(previous_target or 0.0)
    goal_floor = _goal_band_phase_floor(goal_band, phase)
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    moderate_fatigue = bool(weekly_goal.get("moderate_fatigue"))
    atl_spike = bool(weekly_goal.get("atl_spike"))

    if week_type == "cutback":
        return round(max(goal_floor * 0.82, previous_target * 0.84), 1)
    if phase == "taper":
        weeks_to_race = float(weekly_goal.get("weeks_to_race") or 0.0)
        taper_factor = 0.72 if weeks_to_race <= 1.5 else 0.84
        return round(max(goal_floor, previous_target * taper_factor), 1)
    if rebuild_mode or high_fatigue or atl_spike:
        ramp_pct = 0.00
        step_km = 2.0
    elif moderate_fatigue:
        ramp_pct = 0.04
        step_km = 2.0
    elif phase == "base":
        ramp_pct = 0.08
        step_km = 4.0
    elif phase == "build":
        ramp_pct = 0.10
        step_km = 5.0
    else:
        ramp_pct = 0.08
        step_km = 4.0

    capped_target = previous_target * (1.0 + ramp_pct)
    stepped_target = previous_target + step_km
    return round(min(max(goal_floor, stepped_target), capped_target), 1)


def build_progression_weeks(weekly_goal, long_run, weeks=8):
    """Project a deterministic sequence of weeks from the current state.

    Week 0 reflects the current deterministic template. Future weeks progress
    using explicit rules for phase transitions, cutback rhythm, safe ramping,
    and long-run ladder steps. This keeps the plan reproducible while moving
    beyond a single isolated week.
    """
    weeks = max(1, int(weeks or 1))
    base_weekly_goal = dict(weekly_goal or {})
    base_long_run = dict(long_run or {})
    goal_band = _goal_band(base_weekly_goal)
    rebuild_mode = bool(base_weekly_goal.get("rebuild_mode"))
    race_date_value = _coerce_date(base_weekly_goal.get("race_date"))

    week_start_value = base_weekly_goal.get("week_start")
    if isinstance(week_start_value, str):
        week_start_value = date.fromisoformat(week_start_value)
    if not isinstance(week_start_value, date):
        week_start_value = date.today()

    current_week_type = _progression_week_type(0, base_weekly_goal.get("phase") or _phase_for_weeks_to_race(base_weekly_goal.get("weeks_to_race"), rebuild_mode), base_weekly_goal.get("weeks_to_race"), rebuild_mode)
    current_week_goal = {
        **base_weekly_goal,
        "progression_week_index": 0,
        "progression_week_type": current_week_type,
    }
    current_template = build_weekly_plan_template(current_week_goal, base_long_run)
    template_week_target = round(
        sum(float(day["target_km"] or 0.0) for day in current_template.values() if day["workout_type"] == "RUN"),
        1,
    )
    template_long_target = round(float(current_template[6]["target_km"] or 0.0), 1)
    current_week_target = round(float(base_weekly_goal.get("anchor_weekly_target_km") or template_week_target), 1)
    current_long_target = round(float(base_weekly_goal.get("anchor_long_run_km") or template_long_target), 1)
    current_weeks_to_race = float(base_weekly_goal.get("weeks_to_race") or 0.0)
    current_phase = base_weekly_goal.get("phase") or _phase_for_weeks_to_race(current_weeks_to_race, rebuild_mode)

    progression = [
        {
            "week_index": 0,
            "week_start": week_start_value,
            "weeks_to_race": current_weeks_to_race,
            "phase": current_phase,
            "week_type": current_week_type,
            "weekly_target_km": current_week_target,
            "long_run_km": current_long_target,
            "long_run_variant": long_run_variant_for_week(
                current_phase,
                _progression_week_type(0, current_phase, current_weeks_to_race, rebuild_mode),
                0,
                current_long_target,
                {**base_weekly_goal, "phase": current_phase, "weeks_to_race": current_weeks_to_race},
                previous_week_type=None,
            ),
            "template": current_template,
        }
    ]

    effective_base = float(base_long_run.get("effective_longest_km") or 0.0)
    if effective_base <= 0:
        effective_base = effective_long_run_base_km(base_long_run, week_start_value)
    projected_longest = max(effective_base, current_long_target)
    previous_target = current_week_target

    for offset in range(1, weeks):
        projected_week_start = week_start_value + timedelta(days=7 * offset)
        if race_date_value:
            weeks_left = max(0.0, (race_date_value - projected_week_start).days / 7.0)
        else:
            weeks_left = max(0.0, current_weeks_to_race - offset)
        phase = _phase_for_weeks_to_race(weeks_left, rebuild_mode and offset <= 2)
        week_type = _progression_week_type(offset, phase, weeks_left, rebuild_mode and offset <= 2)
        planner_phase = "recovery" if week_type == "cutback" else phase
        candidate_weekly_goal = {
            **base_weekly_goal,
            "phase": planner_phase,
            "progression_week_index": offset,
            "progression_week_type": week_type,
            "weeks_to_race": weeks_left,
            "week_start": projected_week_start.isoformat(),
            "weekly_goal_km": _projected_weekly_target_seed(previous_target, goal_band, planner_phase, week_type, {**base_weekly_goal, "phase": planner_phase, "weeks_to_race": weeks_left}),
            # After the first projected week, assume future planning is not already
            # constrained by the current week's transient fatigue spike.
            "high_fatigue": bool(base_weekly_goal.get("high_fatigue")) if offset == 1 else False,
            "moderate_fatigue": bool(base_weekly_goal.get("moderate_fatigue")) if offset == 1 else False,
            "atl_spike": bool(base_weekly_goal.get("atl_spike")) if offset == 1 else False,
            "prior_avg_km": round(max(_recent_weekly_anchor(base_weekly_goal, previous_target), previous_target * 0.85), 1),
            "current_longest_km": projected_longest,
        }

        if week_type == "cutback":
            next_long_milestone = max(14.0, round(projected_longest * 0.82, 1))
        elif phase == "taper":
            if weeks_left <= 1.5:
                next_long_milestone = max(0.0, round(projected_longest * 0.35, 1))
            elif weeks_left <= 2.5:
                next_long_milestone = max(12.0, round(projected_longest * 0.60, 1))
            else:
                next_long_milestone = max(14.0, round(projected_longest * 0.78, 1))
        else:
            next_long_milestone = _next_progression_long_milestone(projected_longest)

        candidate_long_run = {
            "longest_km": projected_longest,
            "next_milestone_km": next_long_milestone,
        }
        template = build_weekly_plan_template(candidate_weekly_goal, candidate_long_run)
        weekly_target = round(
            sum(float(day["target_km"] or 0.0) for day in template.values() if day["workout_type"] == "RUN"),
            1,
        )
        long_target = round(float(template[6]["target_km"] or 0.0), 1)

        progression.append(
            {
                "week_index": offset,
                "week_start": projected_week_start,
                "weeks_to_race": weeks_left,
                "phase": phase,
                "week_type": week_type,
                "weekly_target_km": weekly_target,
                "long_run_km": long_target,
                "long_run_variant": long_run_variant_for_week(
                    phase,
                    week_type,
                    offset,
                    long_target,
                    candidate_weekly_goal,
                    previous_week_type=progression[-1]["week_type"],
                ),
                "template": template,
            }
        )
        if week_type != "cutback" and phase != "taper":
            projected_longest = max(projected_longest, long_target)
        previous_target = weekly_target

    return progression
