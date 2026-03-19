from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

from ppi.services.load_engine import classify_run_intensity, load_model, running_stress_score
from ppi.services.plan_engine import apply_adaptive_plan, build_weekly_plan_template, classify_run_completion, training_consistency_score
from ppi.services.prediction_engine import marathon_prediction_seconds


# ── DataQualityReport helpers ────────────────────────────────────────────────

def _make_activity(days_ago, distance_km=8.0):
    """Return a mock Activity with .date and .distance_km."""
    a = MagicMock()
    a.date = datetime.utcnow() - timedelta(days=days_ago)
    a.distance_km = distance_km
    return a


def _make_runs(pattern):
    """
    Build a list of mock activities from a pattern list of (days_ago, km) tuples.
    """
    return [_make_activity(d, km) for d, km in pattern]


def _patch_dq(runs, goal=None):
    """
    Context manager that patches Activity.query and Goal.query so
    DataQualityReport can be instantiated without a real DB.
    """
    mock_activity_query = MagicMock()
    mock_activity_query.filter.return_value.order_by.return_value.all.return_value = runs

    mock_goal_query = MagicMock()
    mock_goal_query.filter_by.return_value.order_by.return_value.first.return_value = goal

    return patch.multiple(
        "ppi.services.data_quality",
        Activity=MagicMock(query=mock_activity_query),
        Goal=MagicMock(query=mock_goal_query),
    )


class DummyLog:
    def __init__(self, workout_type, status):
        self.workout_type = workout_type
        self.status = status


def test_plan_engine_caps_completion_and_tracks_extra_distance():
    status, pct, extra = classify_run_completion(6.3, 4.0)
    assert status == "completed"
    assert pct == 100
    assert extra == 2.3


def test_plan_engine_advances_long_run_to_next_ladder_step():
    # With longest=18.3km completed, ladder should target 21km next step.
    # The CTL-based template does not cap the long run by weekly volume.
    weekly_goal = {"weekly_goal_km": 28.0, "phase": "peak", "rebuild_mode": False}
    long_run = {"longest_km": 18.3, "next_milestone_km": 12.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    long_target = float(plan[6]["target_km"])
    assert long_target >= 18.3  # never regresses below current longest
    assert long_target >= 21.0  # advances to next step in ladder


def test_base_plan_targets_next_milestone_regardless_of_weekly_volume():
    # Long run advances to next milestone (21km) even if weekly target < long/0.35.
    weekly_goal = {"weekly_goal_km": 40.0, "phase": "base", "rebuild_mode": False, "weeks_to_race": 23.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 18.3, "next_milestone_km": 21.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    long_target = float(plan[6]["target_km"])
    assert long_target >= 21.0


def test_base_plan_does_not_regress_long_run_for_established_runner():
    weekly_goal = {"weekly_goal_km": 40.0, "phase": "base", "rebuild_mode": False, "weeks_to_race": 23.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 18.3, "next_milestone_km": 21.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert float(plan[6]["target_km"]) >= 18.3


def test_training_consistency_score_uses_last_planned_runs():
    logs = [DummyLog("RUN", "completed"), DummyLog("RUN", "completed"), DummyLog("RUN", "missed"), DummyLog("STRENGTH", "completed")]
    assert training_consistency_score(logs) == 67


def test_training_consistency_counts_moved_session_as_completed():
    logs = [DummyLog("RUN", "completed"), DummyLog("RUN", "moved"), DummyLog("RUN", "missed")]
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
    # Fixed template: TUE is always Tempo Run regardless of phase
    assert plan[1]["session"] == "Tempo Run"
    # Taper phase still cuts the long run down
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


def test_recovery_week_long_run_uses_ladder_progression():
    # build_weekly_plan_template calls _next_long_run_target with apply_capacity_cap=False,
    # so recovery phase falls through to normal ladder progression (next step after 28 = 32).
    # The 78% cutback only applies when apply_capacity_cap=True (adaptive plan runtime).
    weekly_goal = {"weekly_goal_km": 56.0, "phase": "recovery", "rebuild_mode": False, "weeks_to_race": 8.0, "race_distance_km": 42.195}
    long_run = {"longest_km": 28.0, "next_milestone_km": 30.0}
    plan = build_weekly_plan_template(weekly_goal, long_run)
    assert float(plan[6]["target_km"]) == 32.0


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


# ── DataQualityReport tests ───────────────────────────────────────────────────

def test_data_quality_no_data():
    """No activities → no_data confidence, is_sufficient=False, warning banner."""
    from ppi.services.data_quality import DataQualityReport

    with _patch_dq(runs=[]):
        dq = DataQualityReport(user_id=1)

    assert dq.confidence == "no_data"
    assert dq.is_sufficient is False
    assert dq.show_banner is True
    assert dq.banner["type"] == "warning"
    assert dq.to_dict()["total_runs"] == 0


def test_data_quality_insufficient():
    """2 weeks of data → insufficient confidence, is_sufficient=False."""
    from ppi.services.data_quality import DataQualityReport

    # Two runs spread ~14 days apart, both in last 4 weeks (span < 4 weeks)
    runs = _make_runs([(21, 10.0), (7, 8.0)])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    assert dq.confidence == "insufficient"
    assert dq.is_sufficient is False
    assert dq.show_banner is True
    assert dq.to_dict()["data_span_weeks"] < 4


def test_data_quality_low_confidence():
    """4+ weeks span, 2+ active weeks → low confidence but is_sufficient=True."""
    from ppi.services.data_quality import DataQualityReport

    # Runs on days 35, 21, 14, 7 — spans 28 days = 4 weeks, 4 distinct weeks
    runs = _make_runs([(35, 12.0), (21, 10.0), (14, 8.0), (7, 9.0)])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    assert dq.confidence == "low"
    assert dq.is_sufficient is True
    assert dq.to_dict()["data_span_weeks"] >= 4


def test_data_quality_high_confidence():
    """12+ weeks of data, 3+ recent active weeks → high confidence, no banner."""
    from ppi.services.data_quality import DataQualityReport

    # Weekly runs for 13 weeks — oldest first so span computes correctly
    runs = _make_runs([(d, 10.0) for d in range(90, -1, -7)])  # 13 runs over 12 weeks

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    assert dq.confidence == "high"
    assert dq.is_sufficient is True
    assert dq.show_banner is False
    assert dq.banner["message"] == ""
