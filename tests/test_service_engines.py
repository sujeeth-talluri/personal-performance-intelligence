from datetime import date

from ppi.services.load_engine import classify_run_intensity, load_model, running_stress_score
from ppi.services.plan_engine import apply_adaptive_plan, build_weekly_plan_template, classify_run_completion, training_consistency_score
from ppi.services.prediction_engine import marathon_prediction_seconds


class DummyLog:
    def __init__(self, workout_type, status):
        self.workout_type = workout_type
        self.status = status


def test_plan_engine_caps_completion_and_tracks_extra_distance():
    status, pct, extra = classify_run_completion(6.3, 4.0)
    assert status == "completed"
    assert pct == 100
    assert extra == 2.3


def test_plan_engine_respects_long_run_share():
    weekly_goal = {"weekly_goal_km": 28.0, "phase": "peak", "rebuild_mode": False}
    long_run = {"longest_km": 18.3, "next_milestone_km": 12.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    planned_km = sum(float(item.get("target_km") or 0.0) for item in plan.values() if item["workout_type"] == "RUN")
    long_target = float(plan[6]["target_km"])
    assert long_target <= planned_km * 0.35 + 0.2


def test_training_consistency_score_uses_last_planned_runs():
    logs = [DummyLog("RUN", "completed"), DummyLog("RUN", "completed"), DummyLog("RUN", "missed"), DummyLog("STRENGTH", "completed")]
    assert training_consistency_score(logs) == 67


def test_load_engine_uses_distance_and_intensity():
    run = {"type": "run", "distance_km": 10.0, "moving_time_sec": 3600, "pace_sec_per_km": 360.0, "avg_hr": 150, "elevation_gain": 50}
    stress = running_stress_score(run, 320.0)
    assert stress > 10.0


def test_load_engine_returns_ctl_atl_tsb():
    activities = [
        {"date": date(2026, 3, 1), "type": "run", "distance_km": 10.0, "moving_time_sec": 3600, "pace_sec_per_km": 360.0, "avg_hr": 150, "elevation_gain": 0, "intensity": "easy"},
        {"date": date(2026, 3, 2), "type": "run", "distance_km": 12.0, "moving_time_sec": 4200, "pace_sec_per_km": 350.0, "avg_hr": 155, "elevation_gain": 0, "intensity": "aerobic"},
    ]
    model = load_model(activities, date(2026, 3, 16), 320.0, days=7)
    assert "ctl_today" in model and "atl_today" in model and "tsb_today" in model


def test_prediction_engine_returns_projection_with_valid_metrics():
    metrics = {
        "pace_medium": 320.0,
        "pace_long": 340.0,
        "vo2max_estimate": 48.0,
        "recent_race_runs": [],
        "medium_runs": [
            {"distance_km": 10.0, "moving_time_sec": 3200, "intensity": "tempo"},
        ],
        "marathon_specific_runs": [
            {"distance_km": 16.0, "moving_time_sec": 5600, "pace_sec_per_km": 350.0, "intensity": "marathon_specific"},
        ],
        "race_simulation_runs": [],
        "long_runs": [
            {"distance_km": 24.0, "moving_time_sec": 9000, "pace_sec_per_km": 375.0, "intensity": "steady_long"},
        ],
        "goal_marathon_pace_sec_per_km": 330.0,
        "endurance": {"fri": None, "adi": 6.0},
        "weekly": {"prior_avg_km": 58.0, "completed_km": 52.0},
        "rebuild_mode": False,
        "tsb_proxy": -5.0,
        "fatigue_ratio": 1.05,
        "phase": "build",
    }
    prediction = marathon_prediction_seconds(metrics)
    assert prediction is not None
    assert prediction > 0


def test_load_engine_classifies_marathon_specific_long_run():
    run = {"distance_km": 24.0, "pace_sec_per_km": 330.0, "avg_hr": 155}
    assert classify_run_intensity(run, 330.0) == "marathon_specific_long"


def test_adaptive_plan_converts_next_run_to_recovery_when_tsb_low():
    plan_items = [
        {"date": date(2026, 3, 16), "workout_type": "RUN", "session": "Easy Run", "planned_km": 8.0, "planned": "8 km", "status": "planned", "actual_km": None},
        {"date": date(2026, 3, 17), "workout_type": "RUN", "session": "Long Run", "planned_km": 20.0, "planned": "20 km", "status": "planned", "actual_km": None},
    ]
    weekly_goal = {"weekly_goal_km": 42.0, "phase": "build", "high_fatigue": False, "moderate_fatigue": True, "atl_spike": False, "allow_progression": False, "rebuild_mode": False, "max_safe_run": 20.0, "long_run_failed_recent": False}
    adapted = apply_adaptive_plan(plan_items, date(2026, 3, 16), weekly_goal)
    assert adapted[0]["session"] == "Recovery Run"


def test_adaptive_plan_allows_small_progression_when_fresh():
    plan_items = [
        {"date": date(2026, 3, 16), "workout_type": "RUN", "session": "Aerobic Run", "planned_km": 10.0, "planned": "10 km", "status": "planned", "actual_km": None},
        {"date": date(2026, 3, 17), "workout_type": "RUN", "session": "Long Run", "planned_km": 18.0, "planned": "18 km", "status": "planned", "actual_km": None},
    ]
    weekly_goal = {"weekly_goal_km": 46.0, "phase": "build", "high_fatigue": False, "moderate_fatigue": False, "atl_spike": False, "allow_progression": True, "rebuild_mode": False, "max_safe_run": 20.0, "long_run_failed_recent": False}
    adapted = apply_adaptive_plan(plan_items, date(2026, 3, 16), weekly_goal)
    assert adapted[0]["planned_km"] >= 10.0 or adapted[1]["planned_km"] >= 18.0


def test_taper_plan_reduces_long_run_and_keeps_specificity():
    weekly_goal = {"weekly_goal_km": 40.0, "phase": "taper", "rebuild_mode": False}
    long_run = {"longest_km": 30.0, "next_milestone_km": 32.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert plan[1]["session"] == "Marathon Pace Run"
    assert plan[6]["target_km"] <= 20.0


def test_peak_plan_can_progress_to_marathon_specific_long_run():
    weekly_goal = {"weekly_goal_km": 84.0, "phase": "peak", "rebuild_mode": False, "weeks_to_race": 7.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 24.0, "next_milestone_km": 28.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert float(plan[6]["target_km"]) >= 28.0


def test_peak_plan_can_progress_to_30k_when_volume_supports_it():
    weekly_goal = {"weekly_goal_km": 88.0, "phase": "peak", "rebuild_mode": False, "weeks_to_race": 5.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 28.0, "next_milestone_km": 30.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert float(plan[6]["target_km"]) >= 30.0


def test_peak_plan_can_progress_to_32k_when_runner_is_ready():
    weekly_goal = {"weekly_goal_km": 94.0, "phase": "peak", "rebuild_mode": False, "weeks_to_race": 4.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 30.0, "next_milestone_km": 32.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert float(plan[6]["target_km"]) >= 32.0


def test_recovery_week_cuts_back_long_run_after_peak():
    weekly_goal = {"weekly_goal_km": 56.0, "phase": "recovery", "rebuild_mode": False, "weeks_to_race": 8.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 28.0, "next_milestone_km": 30.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert float(plan[6]["target_km"]) < 28.0


def test_race_week_marks_race_day_on_actual_race_date():
    weekly_goal = {
        "weekly_goal_km": 32.0,
        "phase": "taper",
        "rebuild_mode": False,
        "race_date": date(2026, 8, 30),
        "race_distance_km": 42.195,
        "week_start": date(2026, 8, 24),
    }
    long_run = {"longest_km": 30.0, "next_milestone_km": 32.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert plan[6]["session"] == "Race Day"
    assert plan[6]["workout_type"] == "RUN"
    assert plan[6]["target_km"] == 42.195
