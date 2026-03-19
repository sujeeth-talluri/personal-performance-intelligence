from datetime import date


LONG_RUN_LADDER = [21.0, 24.0, 28.0, 32.0]


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
    if phase == "recovery":
        return max(14.0, min(max(16.0, round(longest_km * 0.78, 1)), round(weekly_target * 0.30, 1), practical_peak))
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
    """Build a fixed 7-day training template.

    Weekly structure (Mon=0 … Sun=6):
        MON: Easy Run    — weekly_target × 0.20
        TUE: Tempo Run   — weekly_target × 0.15  (marathon pace + 20 s/km)
        WED: Strength    — gym / cross-training
        THU: Easy Run    — weekly_target × 0.20
        FRI: Strength    — gym / cross-training
        SAT: Easy Run    — weekly_target × 0.20
        SUN: Long Run    — next ladder distance

    Weekly target is driven by CTL (chronic training load):
        CTL < 30   →  45 km
        CTL 30–45  →  55 km
        CTL 45–60  →  65 km
        CTL ≥ 60   →  75 km
    """
    ctl = float(weekly_goal.get("ctl_proxy") or 0.0)
    if ctl < 30:
        weekly_target = 45.0
    elif ctl < 45:
        weekly_target = 55.0
    elif ctl < 60:
        weekly_target = 65.0
    else:
        weekly_target = 75.0

    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    longest_km = float(long_run.get("longest_km") or 0.0)
    next_milestone = float(long_run.get("next_milestone_km") or max(22.0, min(32.0, longest_km + 2.0)))

    # Long run target from progression ladder — not capacity-capped.
    # The CTL-based weekly_target drives easy/tempo volume only; the long
    # run is determined purely by the progression ladder so it always
    # advances to the next milestone regardless of total weekly km.
    long_target = _next_long_run_target(
        "rebuild" if rebuild_mode else phase,
        longest_km,
        next_milestone,
        weekly_target,
        weekly_goal,
        apply_capacity_cap=False,
    )

    easy_km  = round(weekly_target * 0.20, 1)
    tempo_km = round(weekly_target * 0.15, 1)

    template = {
        0: {"workout_type": "RUN",      "session": "Easy Run",  "target_km": easy_km,   **plan_meta_for_session("Easy Run")},
        1: {"workout_type": "RUN",      "session": "Tempo Run", "target_km": tempo_km,  **plan_meta_for_session("Tempo Run")},
        2: {"workout_type": "STRENGTH", "session": "Strength",  "target_km": None,      **plan_meta_for_session("Strength")},
        3: {"workout_type": "RUN",      "session": "Easy Run",  "target_km": easy_km,   **plan_meta_for_session("Easy Run")},
        4: {"workout_type": "STRENGTH", "session": "Strength",  "target_km": None,      **plan_meta_for_session("Strength")},
        5: {"workout_type": "RUN",      "session": "Easy Run",  "target_km": easy_km,   **plan_meta_for_session("Easy Run")},
        6: {"workout_type": "RUN",      "session": "Long Run",  "target_km": long_target, **plan_meta_for_session("Long Run")},
    }
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
    missed_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and item["status"] == "missed"]
    partial_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and item["status"] == "partial"]
    overperformed_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] < today_local and (item.get("extra_km") or 0.0) >= 1.0]
    completed_run_km = sum(item["actual_km"] or 0.0 for item in plan_items if item["workout_type"] == "RUN" and item["actual_km"])
    future_runs = [item for item in plan_items if item["workout_type"] == "RUN" and item["date"] >= today_local and item["status"] == "planned"]
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
            elif item["session"] == "Long Run":
                item["planned_km"] = round(min(max_safe_run, max(14.0, (item["planned_km"] or 0.0) * 0.85)), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery week trims long-run stress."

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
            increase = 1.0 if next_key["session"] == "Long Run" else 0.5
            next_key["planned_km"] = round(min(max_safe_run if next_key["session"] == "Long Run" else next_key["planned_km"] + increase, (next_key["planned_km"] or 0.0) + increase), 1)
            next_key["planned"] = f"{int(round(next_key['planned_km']))} km"
            next_key["adaptive_note"] = "Small progression allowed because fatigue is low and consistency is good."

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
        item.update(plan_meta_for_session(item["session"]))
    return plan_items


def training_consistency_score(logs):
    planned_runs = [log for log in logs if getattr(log, "workout_type", None) == "RUN"]
    completed_runs = [log for log in planned_runs if getattr(log, "status", None) in {"completed", "moved"}]
    if not planned_runs:
        return 0
    return int(round((len(completed_runs) / len(planned_runs)) * 100))
