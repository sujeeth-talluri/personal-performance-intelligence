def goal_marathon_pace(weekly_goal):
    return float(weekly_goal.get("goal_marathon_pace_sec_per_km") or 0.0)


def plan_meta_for_session(session_name):
    catalog = {
        "Long Run": {"intensity": "long_run", "importance": "High", "purpose": "Build marathon endurance and fueling durability."},
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
    weekly_target = max(18.0, float(weekly_goal.get("weekly_goal_km", 18.0)))
    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    longest_km = float(long_run.get("longest_km") or 0.0)
    next_milestone = float(long_run.get("next_milestone_km") or max(22.0, min(32.0, longest_km + 2.0)))

    def phase_targets(target):
        if rebuild_mode:
            return {"long_target": max(14.0, min(18.0, target * 0.28)), "quality_target": max(6.0, min(8.0, round(target * 0.14, 1))), "aerobic_target": max(6.0, min(10.0, round(target * 0.15, 1))), "easy_one": max(5.0, min(8.0, round(target * 0.12, 1))), "tuesday_session": "Aerobic Run", "thursday_session": "Aerobic Run"}
        if phase == "recovery":
            return {"long_target": max(14.0, min(20.0, target * 0.28)), "quality_target": max(5.0, min(8.0, round(target * 0.12, 1))), "aerobic_target": max(6.0, min(10.0, round(target * 0.14, 1))), "easy_one": max(5.0, min(8.0, round(target * 0.12, 1))), "tuesday_session": "Aerobic Run", "thursday_session": "Steady Run"}
        if phase == "taper":
            return {"long_target": max(12.0, min(20.0, target * 0.28)), "quality_target": max(6.0, min(10.0, round(target * 0.15, 1))), "aerobic_target": max(6.0, min(10.0, round(target * 0.14, 1))), "easy_one": max(5.0, min(8.0, round(target * 0.12, 1))), "tuesday_session": "Marathon Pace Run", "thursday_session": "Recovery Run"}
        if phase == "base":
            return {"long_target": max(16.0, min(24.0, min(next_milestone, target * 0.33))), "quality_target": max(6.0, min(10.0, round(target * 0.15, 1))), "aerobic_target": max(8.0, min(14.0, round(target * 0.18, 1))), "easy_one": max(6.0, min(10.0, round(target * 0.14, 1))), "tuesday_session": "Aerobic Run", "thursday_session": "Steady Run"}
        if phase == "peak":
            return {"long_target": max(18.0, min(32.0, min(next_milestone, target * 0.35))), "quality_target": max(8.0, min(16.0, round(target * 0.18, 1))), "aerobic_target": max(8.0, min(14.0, round(target * 0.16, 1))), "easy_one": max(6.0, min(10.0, round(target * 0.12, 1))), "tuesday_session": "Speed Session", "thursday_session": "Marathon Pace Run"}
        return {"long_target": max(18.0, min(28.0, min(next_milestone, target * 0.35))), "quality_target": max(8.0, min(14.0, round(target * 0.18, 1))), "aerobic_target": max(8.0, min(14.0, round(target * 0.17, 1))), "easy_one": max(6.0, min(10.0, round(target * 0.13, 1))), "tuesday_session": "Speed Session", "thursday_session": "Marathon Pace Run"}

    targets = phase_targets(weekly_target)
    weekly_target = max(weekly_target, round(targets["long_target"] / 0.35, 1))
    targets = phase_targets(weekly_target)
    long_target = targets["long_target"]
    quality_target = targets["quality_target"]
    aerobic_target = targets["aerobic_target"]
    easy_one = targets["easy_one"]
    tuesday_session = targets["tuesday_session"]
    thursday_session = targets["thursday_session"]
    remaining = max(6.0, weekly_target - (long_target + quality_target + aerobic_target + easy_one))
    easy_two = max(6.0, min(14.0, round(remaining, 1)))
    return {
        0: {"workout_type": "RUN", "session": "Easy Run", "target_km": easy_one, **plan_meta_for_session("Easy Run")},
        1: {"workout_type": "RUN", "session": tuesday_session, "target_km": aerobic_target if tuesday_session == "Aerobic Run" else quality_target, **plan_meta_for_session(tuesday_session)},
        2: {"workout_type": "STRENGTH", "session": "Strength", "target_km": None, **plan_meta_for_session("Strength")},
        3: {"workout_type": "RUN", "session": thursday_session, "target_km": quality_target, **plan_meta_for_session(thursday_session)},
        4: {"workout_type": "STRENGTH", "session": "Strength", "target_km": None, **plan_meta_for_session("Strength")},
        5: {"workout_type": "RUN", "session": "Easy Run", "target_km": easy_two, **plan_meta_for_session("Easy Run")},
        6: {"workout_type": "RUN", "session": "Long Run", "target_km": long_target, **plan_meta_for_session("Long Run")},
    }


def apply_adaptive_plan(plan_items, today_local, weekly_goal):
    weekly_goal_km = float(weekly_goal.get("weekly_goal_km") or 0.0)
    phase = weekly_goal.get("phase", "build")
    rebuild_mode = bool(weekly_goal.get("rebuild_mode"))
    max_safe_run = float(weekly_goal.get("max_safe_run") or max(10.0, weekly_goal_km * 0.35))
    long_run_failed_recent = bool(weekly_goal.get("long_run_failed_recent"))
    high_fatigue = bool(weekly_goal.get("high_fatigue"))
    moderate_fatigue = bool(weekly_goal.get("moderate_fatigue"))
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

    if high_fatigue:
        for item in future_runs:
            if item["session"] in {"Easy Run", "Aerobic Run"}:
                item["session"] = "Recovery Run"
                item["planned_km"] = round(max(4.0, (item["planned_km"] or 0.0) * 0.75), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Recovery inserted to control fatigue."
                item.update(plan_meta_for_session(item["session"]))
                break
        for item in future_runs:
            if item["session"] == "Long Run":
                item["planned_km"] = round(min(max_safe_run, max(12.0, (item["planned_km"] or 0.0) * 0.9)), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                if not item.get("adaptive_note"):
                    item["adaptive_note"] = "Long run trimmed slightly to keep fatigue under control."
    elif moderate_fatigue:
        for item in future_runs:
            if item["session"] in {"Tempo Run", "Speed Session", "Marathon Pace Run"}:
                item["planned_km"] = round(max(6.0, (item["planned_km"] or 0.0) * 0.9), 1)
                item["planned"] = f"{int(round(item['planned_km']))} km"
                item["adaptive_note"] = "Quality session volume trimmed to keep fatigue stable."
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
    completed_runs = [log for log in planned_runs if getattr(log, "status", None) == "completed"]
    if not planned_runs:
        return 0
    return int(round((len(completed_runs) / len(planned_runs)) * 100))
