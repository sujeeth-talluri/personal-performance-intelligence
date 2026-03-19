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


# ── Scenario tests ────────────────────────────────────────────────────────────
#
# Each scenario uses _make_runs with runs in ascending date order (oldest first),
# matching the ORDER BY date ASC the real query applies.
#
# Span arithmetic:  span_days = (newest_date - oldest_date).days
#                   span_weeks = span_days / 7
#
# Recent window:    a.date >= today - 28 days  (four_weeks_ago)
# Distinct weeks:   ISO-week Monday; runs spaced 7 days apart are always in
#                   different ISO weeks.
# Long-run gate:    distance_km >= 15 triggers long_run detection.
#
# Confidence tiers (both conditions required):
#   high:         span >= 12w AND recent_active_weeks >= 3
#   medium:       span >= 8w  AND recent_active_weeks >= 2
#   low:          span >= 4w  AND recent_active_weeks >= 2
#   insufficient: anything below low, OR recent_active_weeks < 2
#   no_data:      zero activities


def test_dq_scenario1_brand_new_user():
    """S1 — Zero activities: no_data, not sufficient, warning with sync action."""
    from ppi.services.data_quality import DataQualityReport

    with _patch_dq(runs=[]):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "no_data"
    assert r["is_sufficient"] is False
    assert r["total_runs"] == 0
    assert r["show_banner"] is True
    assert r["banner"]["type"] == "warning"
    assert r["banner"]["action"] == "Sync Strava Now"


def test_dq_scenario2_one_week_of_data():
    """S2 — 3 runs all within 7 days: span < 1 week → insufficient, warning banner."""
    from ppi.services.data_quality import DataQualityReport

    # oldest=day7, newest=day2 → span = 5/7 = 0.71 weeks < 4
    runs = _make_runs([(7, 6.0), (4, 5.5), (2, 7.0)])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "insufficient"
    assert r["is_sufficient"] is False
    assert r["show_banner"] is True
    assert r["banner"]["type"] == "warning"
    # Banner message must reference the weeks threshold
    assert "week" in r["banner"]["message"].lower()


def test_dq_scenario3_two_weeks_both_active():
    """S3 — 5 runs over 2 weeks: span = 2w < 4w minimum → insufficient."""
    from ppi.services.data_quality import DataQualityReport

    # oldest=day14, newest=day0 → span = 14/7 = 2.0 weeks < 4
    # recent_weeks covers both, but span still fails the gate
    runs = _make_runs([(14, 9.0), (11, 8.0), (7, 10.0), (4, 7.0), (0, 8.0)])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "insufficient"
    assert r["is_sufficient"] is False
    assert r["data_span_weeks"] < 4


def test_dq_scenario4_four_weeks_two_active():
    """S4 — 3 runs over 5w span, 2 of last 4 weeks active: low confidence, sufficient."""
    from ppi.services.data_quality import DataQualityReport

    # oldest=day35, newest=day7 → span = 28/7 = 4.0 weeks ≥ 4
    # recent window (≤28 days): days 14 and 7 → 2 distinct ISO weeks ≥ 2
    runs = _make_runs([(35, 12.0), (14, 10.0), (7, 9.0)])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "low"
    assert r["is_sufficient"] is True
    assert r["show_banner"] is True
    assert r["banner"]["type"] == "info"
    assert r["data_span_weeks"] >= 4
    assert r["recent_weeks_with_data"] >= 2


def test_dq_scenario5_inactive_three_weeks_sick():
    """S5 — 4+ weeks history but ran only 1 week in last 4: insufficient (recent gate fails)."""
    from ppi.services.data_quality import DataQualityReport

    # Runs at weeks 8, 7, 6 ago (outside 28-day window) then one run at day 7
    # oldest=day56, newest=day7 → span = 49/7 = 7.0 weeks ≥ 4
    # recent (≤28 days): only day7 → 1 active week < 2 required
    runs = _make_runs([(56, 10.0), (49, 11.0), (42, 9.0), (7, 6.0)])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "insufficient"
    assert r["is_sufficient"] is False
    assert r["recent_weeks_with_data"] < 2


def test_dq_scenario6_eight_weeks_consistent():
    """S6 — 8 runs across ~9 weeks (6 active weeks): medium confidence, sufficient."""
    from ppi.services.data_quality import DataQualityReport

    # oldest=day63, newest=day2 → span = 61/7 = 8.71 weeks ≥ 8
    # skipped weeks at ~35 and ~42 = 6 of 8 active weeks
    # recent (≤28 days): days 28, 21, 14, 7, 2 → 5 distinct ISO weeks ≥ 2
    runs = _make_runs([
        (63, 10.0), (56, 11.0), (49, 9.0),
        (28, 10.0), (21, 12.0), (14, 9.0), (7, 10.0), (2, 8.0),
    ])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "medium"
    assert r["is_sufficient"] is True
    assert r["show_banner"] is True
    assert r["data_span_weeks"] >= 8
    assert r["recent_weeks_with_data"] >= 2


def test_dq_scenario7_twelve_weeks_very_consistent():
    """S7 — 12 runs over 13 weeks (10 active): high confidence, no banner shown."""
    from ppi.services.data_quality import DataQualityReport

    # oldest=day91, newest=day2 → span = 89/7 = 12.7 weeks ≥ 12
    # skipped weeks at day56 and day28 → 10 of 12 active weeks
    # recent (≤28 days): days 21, 14, 7, 2 → 4 distinct ISO weeks ≥ 3
    runs = _make_runs([
        (91, 10.0), (84, 11.0), (77, 12.0), (70, 10.0),
        (63, 11.0), (49, 10.0), (42, 9.0), (35, 11.0),
        (21, 10.0), (14, 12.0), (7, 10.0), (2, 8.0),
    ])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "high"
    assert r["is_sufficient"] is True
    assert r["show_banner"] is False
    assert r["banner"]["message"] == ""
    assert r["data_span_weeks"] >= 12
    assert r["recent_weeks_with_data"] >= 3


def test_dq_scenario8_stopped_running_six_weeks_ago():
    """S8 — 20 weeks of history but no runs in last 6 weeks: insufficient (recent gate fails)."""
    from ppi.services.data_quality import DataQualityReport

    # Runs from 20 weeks ago to 6 weeks ago, then silence
    # oldest=day140, newest=day42 → span = 98/7 = 14 weeks ≥ 4
    # recent (≤28 days): day42 is 42 > 28 → 0 active weeks in window
    runs = _make_runs([
        (140, 12.0), (112, 11.0), (84, 10.0),
        (63, 11.0), (56, 12.0), (49, 10.0), (42, 9.0),
    ])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "insufficient"
    assert r["is_sufficient"] is False
    assert r["recent_weeks_with_data"] == 0
    assert r["data_span_weeks"] >= 12  # lots of history, but stale


def test_dq_scenario9_sub3_runner_high_volume():
    """S9 — 17 weeks, 80km/week with long runs: high confidence."""
    from ppi.services.data_quality import DataQualityReport

    # Weekly runs for 17 weeks — oldest first
    # oldest=day112, newest=day0 → span = 112/7 = 16.0 weeks ≥ 12
    # recent (≤28 days): days 28, 21, 14, 7, 0 → 5 distinct ISO weeks ≥ 3
    # Include 20km long runs (≥15km) every 3rd week
    pattern = []
    for weeks_ago in range(16, -1, -1):
        days = weeks_ago * 7
        km = 20.0 if weeks_ago % 3 == 0 else 12.0  # long run every 3 weeks
        pattern.append((days, km))
    runs = _make_runs(pattern)

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "high"
    assert r["is_sufficient"] is True
    assert r["long_runs_count"] > 0
    assert r["data_span_weeks"] >= 12
    assert r["recent_weeks_with_data"] >= 3


def test_dq_scenario10_first_timer_no_long_runs():
    """S10 — 5 weeks, 20km/week, all runs short (no run ≥15km): low confidence, missing long-run hint."""
    from ppi.services.data_quality import DataQualityReport

    # oldest=day42, newest=day2 → span = 40/7 = 5.71 weeks ≥ 4 but < 8
    # recent (≤28 days): days 21, 14, 7, 2 → 4 distinct ISO weeks ≥ 2
    # All runs ≤ 10km → long_runs_count = 0 → missing includes long-run guidance
    runs = _make_runs([
        (42, 5.0), (35, 8.0), (28, 6.0),
        (21, 10.0), (14, 7.0), (7, 8.0), (2, 6.0),
    ])

    with _patch_dq(runs=runs):
        dq = DataQualityReport(user_id=1)

    r = dq.to_dict()
    assert r["confidence"] == "low"
    assert r["is_sufficient"] is True
    assert r["long_runs_count"] == 0
    assert any("long run" in m.lower() for m in r["missing"])


# ── ComplianceEngine helpers ──────────────────────────────────────────────────
#
# Mock layers patched at "ppi.services.compliance_engine":
#   WorkoutLog  — planned sessions
#   Activity    — Strava actuals
#   Metric      — ATL/TSB load signals
#   db          — session.query for trend aggregates (4 weekly scalars)
#
# Activity.date must be a real datetime so .date() returns a real date;
# the gap-detection loop relies on this.
#
# Run activities:
#   pace_sec_per_km < 390  → quality effort (tempo / interval)
#   pace_sec_per_km >= 390 → easy effort
#   moving_time = pace_sec_per_km * distance_km


def _make_log(workout_date, session_name, target_km, workout_type="RUN"):
    """Mock WorkoutLog row."""
    log = MagicMock()
    log.workout_date = workout_date
    log.workout_type = workout_type
    log.session_name = session_name
    log.target_distance_km = target_km
    return log


def _make_run(dt, distance_km, pace_sec_per_km=420, activity_type="run"):
    """Mock Activity row — dt must be a real datetime."""
    act = MagicMock()
    act.date = dt
    act.activity_type = activity_type
    act.distance_km = distance_km
    act.moving_time = pace_sec_per_km * distance_km
    return act


class _Col:
    """
    Minimal SQLAlchemy column expression stand-in for tests.

    `MagicMock >= date` raises TypeError in Python 3.9+ because
    MagicMock.__ge__ returns NotImplemented for non-Mock types, and
    datetime.date.__le__(MagicMock) also returns NotImplemented.
    This class prevents that by explicitly handling all comparisons.
    """
    def __eq__(self, other): return MagicMock()
    def __ne__(self, other): return MagicMock()
    def __ge__(self, other): return MagicMock()
    def __le__(self, other): return MagicMock()
    def __gt__(self, other): return MagicMock()
    def __lt__(self, other): return MagicMock()
    def __hash__(self): return id(self)
    def asc(self): return MagicMock()
    def desc(self): return MagicMock()


def _patch_compliance(planned_logs, actual_activities, trend_km=None, atl=0.0, tsb=0.0, ctl=0.0):
    """
    Patch all DB dependencies of ComplianceEngine.

    trend_km: list of 4 floats [last_week, 2w_ago, 3w_ago, 4w_ago]
    """
    if trend_km is None:
        trend_km = [0.0, 0.0, 0.0, 0.0]

    # WorkoutLog — column attrs use _Col so >= / <= with date don't raise TypeError
    mock_wl = MagicMock()
    mock_wl.user_id = _Col()
    mock_wl.workout_date = _Col()
    mock_wl.workout_type = _Col()
    mock_wl.session_name = _Col()
    mock_wl.query.filter.return_value.all.return_value = planned_logs

    # Activity — same treatment for date comparisons
    mock_act = MagicMock()
    mock_act.user_id = _Col()
    mock_act.date = _Col()
    mock_act.activity_type = _Col()
    mock_act.distance_km = _Col()
    mock_act.query.filter.return_value.order_by.return_value.all.return_value = actual_activities

    # Metric (load signals)
    mock_metric_row = MagicMock()
    mock_metric_row.ctl = ctl
    mock_metric_row.atl = atl
    mock_metric_row.tsb = tsb
    mock_metric = MagicMock()
    mock_metric.query.filter_by.return_value.order_by.return_value.first.return_value = mock_metric_row

    # db.session.query(...).filter(...).scalar() — called 4 times for trend loop
    mock_scalar = MagicMock()
    mock_scalar.filter.return_value.scalar.side_effect = list(trend_km)
    mock_db = MagicMock()
    mock_db.session.query.return_value = mock_scalar

    return patch.multiple(
        "ppi.services.compliance_engine",
        WorkoutLog=mock_wl,
        Activity=mock_act,
        Metric=mock_metric,
        db=mock_db,
    )


def _last_week_dates():
    """Return (last_monday, last_sunday) as date objects."""
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday, last_sunday


def _dt(d):
    """Convert date to datetime at midnight."""
    return datetime.combine(d, datetime.min.time())


# ── Compliance scenario tests ─────────────────────────────────────────────────


def test_compliance_scenario1_perfect_week():
    """S1 — 43/45km completed (95.6%): on_track, volume_adjustment=1.05."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [
        _make_log(monday,                   "Easy Run",  9.0),
        _make_log(monday + timedelta(days=1), "Easy Run",  9.0),
        _make_log(monday + timedelta(days=2), "Tempo Run", 9.0),
        _make_log(monday + timedelta(days=4), "Easy Run",  9.0),
        _make_log(monday + timedelta(days=6), "Long Run",  9.0),
    ]
    # 5 easy runs, 43km total, spread across Mon/Tue/Wed/Fri/Sun — max gap = 1
    actual = [
        _make_run(_dt(monday),                   8.5, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=1)), 9.0, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=2)), 8.5, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=4)), 9.0, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=6)), 8.0, pace_sec_per_km=420),
    ]
    # consecutive_good_weeks = 1 (only last week ≥20) → volume_adjustment = 1.05
    with _patch_compliance(planned, actual, trend_km=[43.0, 0.0, 0.0, 0.0]):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "on_track"
    assert r["volume_compliance_pct"] >= 90
    assert r["response"]["volume_adjustment"] == 1.05


def test_compliance_scenario2_fatigue_miss():
    """S2 — 64% volume, ATL=38, TSB=-15: fatigue detected, volume_adjustment=0.85."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [_make_log(monday + timedelta(days=i), "Easy Run", 10.0) for i in range(5)]
    actual = [
        _make_run(_dt(monday),                   8.0,  pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=1)), 8.0,  pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=3)), 8.0,  pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=4)), 8.0,  pace_sec_per_km=420),
    ]
    # actual=32km, planned=50km → 64% < 80%; ATL=38>30, TSB=-15<-10 → fatigue

    with _patch_compliance(planned, actual, atl=38.0, tsb=-15.0):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "fatigue"
    assert r["response"]["volume_adjustment"] == 0.85
    assert "volume" in r["response"]["message"].lower() or "15%" in r["response"]["message"]


def test_compliance_scenario3_illness_five_day_gap():
    """S3 — 8km in 2 days, 5-day consecutive gap: illness_or_life, volume_adjustment=0.70."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [_make_log(monday + timedelta(days=i), "Easy Run", 9.0) for i in range(5)]
    # Only Monday (4km) and Sunday (4km) — Tue through Sat = 5 consecutive rest days
    actual = [
        _make_run(_dt(monday), 4.0, pace_sec_per_km=420),
        _make_run(_dt(sunday), 4.0, pace_sec_per_km=420),
    ]

    with _patch_compliance(planned, actual):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "illness_or_life"
    assert r["max_gap_days"] >= 5
    assert r["response"]["volume_adjustment"] == 0.70


def test_compliance_scenario4_busy_week_time_management():
    """S4 — 28/45km, 3 days active, no long gap, ATL normal: time_management, adjustment=1.0."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [_make_log(monday + timedelta(days=i), "Easy Run", 9.0) for i in range(5)]
    # Mon, Wed, Fri active → max gap = 2 days; 62% volume, ATL=25≤30
    actual = [
        _make_run(_dt(monday),                    9.0, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=2)), 10.0, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=4)),  9.0, pace_sec_per_km=420),
    ]

    with _patch_compliance(planned, actual, atl=25.0, tsb=-2.0):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "time_management"
    assert r["response"]["volume_adjustment"] == 1.0
    assert r["max_gap_days"] < 5


def test_compliance_scenario5_key_sessions_skipped():
    """S5 — 20/45km easy only, no tempo, no long run (44%): key_sessions_skipped."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [
        _make_log(monday,                   "Easy Run",  9.0),
        _make_log(monday + timedelta(days=1), "Easy Run",  9.0),
        _make_log(monday + timedelta(days=2), "Tempo Run", 9.0),    # quality session
        _make_log(monday + timedelta(days=4), "Easy Run",  9.0),
        _make_log(monday + timedelta(days=6), "Long Run",  9.0),    # planned_long = 9km... wait
    ]
    # planned_long = max(9, 9, 9, 9, 9) = 9km; long_run_done requires actual_long ≥ 9*0.90=8.1km
    # Use 3 short easy runs (6+7+7=20km) so actual_long=7 < 8.1 → long_run_done=False
    # pace=420>390 → quality_done=False
    # pct = 20/45 = 44.4% < 50 → time_management guard (50≤pct<90) FAILS → falls to key_sessions_skipped
    # actual_run_days=3, planned_run_days=5; 3≥5*0.6=3 ✓
    actual = [
        _make_run(_dt(monday),                   6.0, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=2)), 7.0, pace_sec_per_km=420),
        _make_run(_dt(monday + timedelta(days=4)), 7.0, pace_sec_per_km=420),
    ]

    with _patch_compliance(planned, actual, atl=20.0, tsb=-2.0):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "key_sessions_skipped"
    assert r["response"]["priority"] == "quality_focus"


def test_compliance_scenario6_complete_miss():
    """S6 — Zero activities: complete_miss, volume_adjustment=0.65."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [_make_log(monday + timedelta(days=i), "Easy Run", 9.0) for i in range(5)]

    with _patch_compliance(planned, actual_activities=[]):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "complete_miss"
    assert r["actual_run_km"] == 0.0
    assert r["response"]["volume_adjustment"] == 0.65


def test_compliance_scenario7_three_consecutive_good_weeks():
    """S7 — On-track + 4 consecutive good weeks: volume_adjustment=1.10 (step up)."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [_make_log(monday + timedelta(days=i), "Easy Run", 9.0) for i in range(5)]
    actual = [
        _make_run(_dt(monday + timedelta(days=i)), 9.0, pace_sec_per_km=420)
        for i in range(5)
    ]
    # trend: [42, 44, 43, 38] — all ≥ 20 → consecutive_good_weeks = 4 ≥ 3
    with _patch_compliance(planned, actual, trend_km=[42.0, 44.0, 43.0, 38.0]):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["miss_reason"]["code"] == "on_track"
    assert r["trend"]["consecutive_good_weeks"] >= 3
    assert r["response"]["volume_adjustment"] == 1.10


def test_compliance_scenario8_declining_trend():
    """S8 — Recent avg 20km vs prior avg 40km: direction='declining'."""
    from ppi.services.compliance_engine import ComplianceEngine

    monday, sunday = _last_week_dates()
    planned = [_make_log(monday + timedelta(days=i), "Easy Run", 9.0) for i in range(5)]
    actual = [
        _make_run(_dt(monday + timedelta(days=i)), 9.0, pace_sec_per_km=420)
        for i in range(5)
    ]
    # trend_km[0]=last week, [1]=2w ago, [2]=3w ago, [3]=4w ago
    # recent_avg = (18+22)/2 = 20; older_avg = (38+42)/2 = 40
    # 20 < 40 * 0.90 = 36 → declining
    with _patch_compliance(planned, actual, trend_km=[18.0, 22.0, 38.0, 42.0]):
        ce = ComplianceEngine(user_id=1)

    r = ce.to_dict()
    assert r["trend"]["direction"] == "declining"


# ── FeasibilityEngine helpers ─────────────────────────────────────────────────
#
# _patch_feasibility mocks Goal.query and Activity.query.
# Goal.id must be _Col so order_by(Goal.id.desc()) doesn't raise TypeError.
# Activity.date must be a real datetime so Python-level filtering works
# (a.date < cutoff, a.date.weekday(), etc.).
#
# VDOT reference values (verified against Jack Daniels formula):
#   _vdot_from_race(42.195, 14400)  → ~38.1  (sub-4 marathon; spec says ~42 — wrong)
#   _vdot_from_race(21.097, 6040)   → ~44.78 (HM 1:40:40)
#   _vdot_from_race(42.195, 10800)  → ~53.54 (sub-3 marathon)
#   _vdot_from_race(42.195, 12600)  → ~44.53 (sub-3:30 marathon)
#   _vdot_from_race(5.0,    1620)   → ~34.97 (5k 27:00)
#   _vdot_from_race(21.097, 5460)   → ~50.33 (HM 1:31:00)
#   _vdot_from_race(42.195, 18000)  → ~28.72 (sub-5 marathon)


def _make_goal_fe(goal_time, race_distance, weeks_to_race,
                  pb_hm=None, pb_5k=None, pb_10k=None, personal_best=None):
    """Mock Goal object for FeasibilityEngine tests."""
    goal = MagicMock()
    goal.race_date = date.today() + timedelta(weeks=weeks_to_race)
    goal.goal_time = goal_time
    goal.race_distance = race_distance
    goal.pb_hm = pb_hm
    goal.pb_5k = pb_5k
    goal.pb_10k = pb_10k
    goal.personal_best = personal_best
    return goal


def _make_feas_run(days_ago, distance_km, pace_sec_per_km=450, activity_type="run"):
    """Mock Activity for FeasibilityEngine. date is a real datetime."""
    act = MagicMock()
    act.date = datetime.utcnow() - timedelta(days=days_ago)
    act.distance_km = distance_km
    act.moving_time = pace_sec_per_km * distance_km
    act.activity_type = activity_type
    return act


def _patch_feasibility(goal, activities):
    """Patch Goal.query and Activity.query for FeasibilityEngine tests."""
    mock_goal_cls = MagicMock()
    mock_goal_cls.id = _Col()
    mock_goal_cls.query.filter_by.return_value.order_by.return_value.first.return_value = goal

    mock_act_cls = MagicMock()
    mock_act_cls.user_id = _Col()
    mock_act_cls.date = _Col()
    mock_act_cls.query.filter.return_value.order_by.return_value.all.return_value = activities

    return patch.multiple(
        "ppi.services.feasibility_engine",
        Goal=mock_goal_cls,
        Activity=mock_act_cls,
    )


# ── Feasibility scenario tests ────────────────────────────────────────────────


def test_feasibility_scenario1_already_capable_sub4():
    """S1 — VDOT 44.78 (HM 1:40:40) vs sub-4 (requires ~38.1): already_capable=True."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    goal = _make_goal_fe("04:00:00", 42.195, weeks_to_race=23, pb_hm="1:40:40")
    # 8 weeks: 1 long run (25km) + 4 easy runs (8km) per week = 57km/wk
    activities = []
    for w in range(8):
        activities.append(_make_feas_run(w * 7 + 6, 25.0, pace_sec_per_km=420))
        for d in range(1, 5):
            activities.append(_make_feas_run(w * 7 + d, 8.0, pace_sec_per_km=450))

    with _patch_feasibility(goal, activities):
        fe = FeasibilityEngine(user_id=1)

    r = fe.to_dict()
    assert r["vdot"]["already_capable"] is True
    assert r["vdot"]["gap"] == 0
    assert r["show_revised_goal"] is False
    assert r["vdot"]["current"] >= 44.0  # from pb_hm "1:40:40"


def test_feasibility_scenario2_vdot_gap_sub3():
    """S2 — VDOT ~50 (HM 1:31:00) vs sub-3 (requires ~53.5): already_capable=False, gap > 0."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    goal = _make_goal_fe("03:00:00", 42.195, weeks_to_race=12, pb_hm="1:31:00")
    # 8 weeks of moderate training (40km/wk)
    activities = [
        _make_feas_run(w * 7 + d, 10.0, pace_sec_per_km=400)
        for w in range(8) for d in range(1, 5)
    ]

    with _patch_feasibility(goal, activities):
        fe = FeasibilityEngine(user_id=1)

    r = fe.to_dict()
    assert r["vdot"]["already_capable"] is False
    assert r["vdot"]["gap"] > 0
    assert r["vdot"]["current"] >= 49.0   # from pb_hm "1:31:00" → ~50.33
    assert r["vdot"]["required"] >= 52.0  # sub-3 requires ~53.54


def test_feasibility_scenario3_already_capable_sub5():
    """S3 — VDOT ~35 (5k 27:00) vs sub-5 marathon (requires ~28.7): already_capable=True."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    goal = _make_goal_fe("05:00:00", 42.195, weeks_to_race=20, pb_5k="0:27:00")
    # 8 weeks of light training (20km/wk)
    activities = [
        _make_feas_run(w * 7 + d, 5.0, pace_sec_per_km=480)
        for w in range(8) for d in range(1, 5)
    ]

    with _patch_feasibility(goal, activities):
        fe = FeasibilityEngine(user_id=1)

    r = fe.to_dict()
    assert r["vdot"]["already_capable"] is True
    assert r["show_revised_goal"] is False
    assert r["vdot"]["current"] >= 33.0   # from pb_5k "0:27:00" → ~34.97
    assert r["vdot"]["required"] <= 30.0  # sub-5 requires ~28.72


def test_feasibility_scenario4_needs_revision_vdot35_sub3_6w():
    """S4 — VDOT ~35, sub-3 goal, 6w out, minimal training: needs_revision, revised_goal shown."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    goal = _make_goal_fe("03:00:00", 42.195, weeks_to_race=6, pb_5k="0:27:00")
    # Minimal training: 4 weeks × 2 runs × 10km = 20km/wk; no long runs
    activities = [
        _make_feas_run(w * 7 + d, 10.0, pace_sec_per_km=480)
        for w in range(4) for d in [2, 5]
    ]

    with _patch_feasibility(goal, activities):
        fe = FeasibilityEngine(user_id=1)

    r = fe.to_dict()
    assert r["assessment"] == "needs_revision"
    assert r["show_revised_goal"] is True
    assert r["revised_goal"] is not None
    assert r["feasibility_score"] < 50
    assert "revised_goal_time" in r["revised_goal"]


def test_feasibility_scenario5_on_track_vdot50_sub330():
    """S5 — VDOT ~50 (HM 1:31:00), sub-3:30, 18w, 65km/wk with long runs: on_track."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    goal = _make_goal_fe("03:30:00", 42.195, weeks_to_race=18, pb_hm="1:31:00")
    # 8 weeks: 1 long run (30km) + 4 runs (8.75km each) = 65km/wk
    activities = []
    for w in range(8):
        activities.append(_make_feas_run(w * 7 + 6, 30.0, pace_sec_per_km=420))
        for d in range(1, 5):
            activities.append(_make_feas_run(w * 7 + d, 8.75, pace_sec_per_km=450))

    with _patch_feasibility(goal, activities):
        fe = FeasibilityEngine(user_id=1)

    r = fe.to_dict()
    assert r["assessment"] == "on_track"
    assert r["feasibility_score"] >= 75
    assert r["vdot"]["already_capable"] is True  # 50.33 > 44.53 (sub-3:30 requirement)


def test_feasibility_scenario6_vdot_math_validation():
    """S6 — VDOT formula produces correct values; sub-4 marathon is ~38 (NOT ~42 as widely assumed)."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    # Construct FeasibilityEngine with no goal — returns _empty_result immediately
    with _patch_feasibility(goal=None, activities=[]):
        fe = FeasibilityEngine(user_id=1)

    # sub-4 marathon (4:00:00 = 14400s): VDOT ≈ 38.1 — not 42 as spec incorrectly states
    vdot_sub4 = fe._vdot_from_race(42.195, 14400)
    assert 37.0 < vdot_sub4 < 40.0, f"sub-4 VDOT should be ~38, got {vdot_sub4}"

    # HM 1:40:40 (6040s): VDOT ≈ 44.78
    vdot_hm = fe._vdot_from_race(21.097, 6040)
    assert 44.0 < vdot_hm < 46.0, f"HM 1:40:40 VDOT should be ~44.78, got {vdot_hm}"

    # Inverse: VDOT 44.78 → marathon prediction ≈ 3:29 (12480–12720s)
    predicted_marathon = fe._vdot_to_race_time(44.78, 42.195)
    assert 12300 < predicted_marathon < 12900, (
        f"VDOT 44.78 marathon should be ~12546s (3:29), got {predicted_marathon}"
    )

    # sub-3 marathon (10800s): VDOT ≈ 53.54
    vdot_sub3 = fe._vdot_from_race(42.195, 10800)
    assert 52.0 < vdot_sub3 < 55.0, f"sub-3 VDOT should be ~53.5, got {vdot_sub3}"


def test_feasibility_scenario7_consistency_score_edge_cases():
    """S7 — consistency_score: 8/8 active weeks → ≥90; alternating → ~50; no runs → 0."""
    from ppi.services.feasibility_engine import FeasibilityEngine

    with _patch_feasibility(goal=None, activities=[]):
        fe = FeasibilityEngine(user_id=1)

    # All 8 weeks active, 25km each, zero variance → score ≈ 100
    all_active = [
        _make_feas_run(days_ago=w * 7 + 1, distance_km=25.0)
        for w in range(8)
    ]
    score_all = fe._consistency_score(all_active)
    assert score_all >= 90.0, f"All-active score should be ≥90, got {score_all}"

    # 4 of 8 weeks active (weeks 0, 2, 4, 6) — base 50%, zero variance within active weeks
    alt_runs = [
        _make_feas_run(days_ago=w * 7 + 1, distance_km=25.0)
        for w in range(0, 8, 2)
    ]
    score_alt = fe._consistency_score(alt_runs)
    assert 40.0 <= score_alt <= 60.0, f"Alternating score should be ~50, got {score_alt}"

    # No runs at all → 0
    score_none = fe._consistency_score([])
    assert score_none == 0.0


# ── AICoachEngine helpers ─────────────────────────────────────────────────────
#
# AICoachEngine() takes no __init__ args — instantiate freely.
# _fallback_weekly, _fallback_progression, _validate_weekly,
# _validate_progression all operate on plain dicts — no DB or Flask context.
# _build_cache_key touches Goal.query and Activity.query — needs patching.
#
# Context dict shape (minimal):
#   compliance:  {"actual_run_km", "trend", "response", "miss_reason"}
#   feasibility: {"readiness": {"current_long_run_km", ...}}
#   runner_profile: {"training_days_per_week", "long_run_day", ...}
#   goal: {"race_date", "goal_time", "goal_distance_km", "weeks_to_race", ...}


def _make_ai_context(
    actual_run_km=38.0,
    current_long_km=22.0,
    training_days=5,
    long_run_day="sunday",
    vol_adjustment=1.0,
    avg_last_2=38.0,
    weeks_to_race=20,
    race_date=None,
    goal_distance_km=42.195,
):
    """Build a minimal context dict for AICoachEngine method tests."""
    from datetime import date, timedelta
    if race_date is None:
        race_date = (date.today() + timedelta(weeks=weeks_to_race)).strftime("%Y-%m-%d")
    return {
        "runner_profile": {
            "training_days_per_week": training_days,
            "long_run_day":           long_run_day,
            "strength_days_per_week": 2,
            "preferred_run_time":     "morning",
            "consistency_level":      "consistent",
            "race_experience":        "multiple",
            "injury_status":          "healthy",
            "injury_area":            "none",
            "goal_priority":          "hit_time",
        },
        "goal": {
            "race_name":       "Test Marathon",
            "race_date":       race_date,
            "goal_time":       "03:45:00",
            "goal_distance_km": goal_distance_km,
            "weeks_to_race":   weeks_to_race,
            "today":           date.today().strftime("%Y-%m-%d"),
            "personal_best":   None,
            "pb_5k": None, "pb_10k": None, "pb_hm": "1:45:00",
        },
        "data_quality": {"confidence": "high", "is_sufficient": True},
        "compliance": {
            "actual_run_km": actual_run_km,
            "planned_run_km": actual_run_km + 5,
            "volume_compliance_pct": 90.0,
            "miss_reason": {"code": "on_track"},
            "trend": {
                "direction": "steady",
                "consecutive_good_weeks": 2,
                "avg_last_2_weeks": avg_last_2,
            },
            "response": {"volume_adjustment": vol_adjustment},
        },
        "feasibility": {
            "feasibility_score": 72.0,
            "assessment_label":  "Achievable",
            "vdot":              {"current": 42.0, "required": 44.0, "already_capable": False},
            "readiness": {
                "current_long_run_km":   current_long_km,
                "required_long_run_km":  35.9,
                "current_weekly_avg_km": actual_run_km,
                "required_peak_weekly_km": 65.0,
            },
            "consistency_score": 75.0,
        },
        "weekly_history":   [],
        "long_run_history": [],
    }


def _patch_ai_cache(goal_mock, activity_mock):
    """Patch Goal and Activity in ai_coach_engine for cache key tests."""
    mock_goal_cls = MagicMock()
    mock_goal_cls.id = _Col()
    mock_goal_cls.query.filter_by.return_value.order_by.return_value.first.return_value = goal_mock

    mock_act_cls = MagicMock()
    mock_act_cls.date = _Col()
    mock_act_cls.query.filter_by.return_value.order_by.return_value.first.return_value = activity_mock

    return patch.multiple(
        "ppi.services.ai_coach_engine",
        Goal=mock_goal_cls,
        Activity=mock_act_cls,
    )


# ── AI Engine scenario tests ───────────────────────────────────────────────────


def test_ai_engine_scenario1_cache_key_generation():
    """S1 — Cache keys differ by user; same inputs → same key; new activity → different key."""
    from ppi.services.ai_coach_engine import AICoachEngine

    engine = AICoachEngine()

    def _goal(gid=1, goal_time="03:45:00", race_date="2026-12-01"):
        g = MagicMock()
        g.id = gid; g.goal_time = goal_time; g.race_date = race_date
        return g

    def _act(dt_str="2026-03-01"):
        a = MagicMock()
        a.date = dt_str
        return a

    goal = _goal()
    act1 = _act("2026-03-01")
    act2 = _act("2026-03-15")  # different last activity date

    # Same inputs → same key
    with _patch_ai_cache(goal, act1):
        key_a = engine._build_cache_key(user_id=1)
        key_b = engine._build_cache_key(user_id=1)
    assert key_a == key_b, "Same inputs must produce same cache key"

    # Different user → different key
    with _patch_ai_cache(goal, act1):
        key_user1 = engine._build_cache_key(user_id=1)
        key_user2 = engine._build_cache_key(user_id=2)
    assert key_user1 != key_user2, "Different users must produce different cache keys"

    # New activity date → different key
    with _patch_ai_cache(goal, act1):
        key_old = engine._build_cache_key(user_id=1)
    with _patch_ai_cache(goal, act2):
        key_new = engine._build_cache_key(user_id=1)
    assert key_old != key_new, "New activity date must invalidate cache key"


def test_ai_engine_scenario2_fallback_weekly_required_fields():
    """S2 — _fallback_weekly() returns a valid plan with all required fields."""
    from ppi.services.ai_coach_engine import AICoachEngine

    engine = AICoachEngine()
    ctx = _make_ai_context(actual_run_km=35.0, current_long_km=20.0)
    plan = engine._fallback_weekly(ctx)

    assert "phase" in plan
    assert "coaching_message" in plan
    assert "this_week" in plan

    this_week = plan["this_week"]
    assert "weekly_target_km" in this_week
    assert "daily_plan" in this_week
    assert "long_run" in this_week
    assert "focus_point" in this_week

    # Weekly target should be reasonable (not zero, not absurd)
    assert 15 <= this_week["weekly_target_km"] <= 85

    # All 7 days present
    days = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
    assert set(this_week["daily_plan"].keys()) == days

    # Long run on preferred day (sunday)
    assert this_week["daily_plan"]["sunday"]["type"] == "long"

    # Strength sessions have km=0
    for day, session in this_week["daily_plan"].items():
        if session["type"] == "strength":
            assert session["km"] == 0


def test_ai_engine_scenario3_fallback_progression_structure():
    """S3 — _fallback_progression() returns correct structure, taper last 3, no km > max_long."""
    from ppi.services.ai_coach_engine import AICoachEngine

    engine = AICoachEngine()
    ctx = _make_ai_context(current_long_km=20.0, weeks_to_race=16)
    progression = engine._fallback_progression(ctx)

    assert isinstance(progression, list)
    assert len(progression) > 0

    required_keys = {"week_number", "week_date", "target_km", "phase", "is_recovery_week"}
    for entry in progression:
        assert required_keys.issubset(entry.keys()), f"Missing keys in {entry}"

    # max_long = min(32, 42.195 * 0.80) = 32.0
    max_long = min(32.0, 42.195 * 0.80)
    for entry in progression:
        assert entry["target_km"] <= max_long + 0.1, (
            f"Week {entry['week_number']} exceeds max_long: {entry['target_km']}"
        )

    # Last 3 items (taper) must be taper phase
    taper_entries = progression[-3:]
    for entry in taper_entries:
        assert entry["phase"] == "taper", (
            f"Expected taper phase, got '{entry['phase']}' on {entry['week_date']}"
        )


def test_ai_engine_scenario4_validate_weekly_10pct_rule():
    """S4 — 10% rule: AI target=100km, actual=38km → validated to ≤41.8km."""
    from ppi.services.ai_coach_engine import AICoachEngine

    engine = AICoachEngine()
    ctx = _make_ai_context(actual_run_km=38.0, avg_last_2=38.0, vol_adjustment=1.0)

    # AI plan with outrageously high weekly target
    ai_plan = {
        "phase": "build",
        "phase_label": "Build",
        "phase_reasoning": "test",
        "week_theme": "test",
        "is_recovery_week": False,
        "this_week": {
            "weekly_target_km": 100.0,  # AI went rogue
            "daily_plan": {
                day: {"type": "easy", "km": 14.0, "pace_guidance": "", "notes": ""}
                for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            },
            "long_run": {"km": 35.0, "pace_guidance": "", "purpose": ""},
            "quality_session": {"day": "tuesday", "type": "tempo", "km": 12.0, "pace_guidance": "", "description": ""},
            "focus_point": "",
            "compliance_response": "",
        },
        "coaching_message": "",
        "alerts": [],
    }

    validated = engine._validate_weekly(ai_plan, ctx)

    # 38 * 1.10 * 1.0 = 41.8
    assert validated["this_week"]["weekly_target_km"] <= 41.9, (
        f"Expected ≤41.8km, got {validated['this_week']['weekly_target_km']}"
    )
    assert validated["validation"]["weekly_target_enforced"] is True


def test_ai_engine_scenario5_validate_long_run_cap():
    """S5 — Long run cap: AI says 35km, current_long=18km → validated to ≤21km."""
    from ppi.services.ai_coach_engine import AICoachEngine

    engine = AICoachEngine()
    ctx = _make_ai_context(actual_run_km=45.0, current_long_km=18.0, avg_last_2=45.0)

    ai_plan = {
        "phase": "peak",
        "phase_label": "Peak",
        "phase_reasoning": "test",
        "week_theme": "test",
        "is_recovery_week": False,
        "this_week": {
            "weekly_target_km": 48.0,
            "daily_plan": {
                "monday":    {"type": "easy",     "km": 8.0,  "pace_guidance": "", "notes": ""},
                "tuesday":   {"type": "tempo",    "km": 10.0, "pace_guidance": "", "notes": ""},
                "wednesday": {"type": "strength", "km": 0,    "pace_guidance": "", "notes": ""},
                "thursday":  {"type": "easy",     "km": 8.0,  "pace_guidance": "", "notes": ""},
                "friday":    {"type": "strength", "km": 0,    "pace_guidance": "", "notes": ""},
                "saturday":  {"type": "easy",     "km": 8.0,  "pace_guidance": "", "notes": ""},
                "sunday":    {"type": "long",     "km": 35.0, "pace_guidance": "", "notes": ""},
            },
            "long_run": {"km": 35.0, "pace_guidance": "", "purpose": ""},
            "quality_session": {"day": "tuesday", "type": "tempo", "km": 10.0, "pace_guidance": "", "description": ""},
            "focus_point": "",
            "compliance_response": "",
        },
        "coaching_message": "",
        "alerts": [],
    }

    validated = engine._validate_weekly(ai_plan, ctx)

    # 18 + 3 = 21km max
    assert validated["this_week"]["long_run"]["km"] <= 21.1, (
        f"Expected ≤21km, got {validated['this_week']['long_run']['km']}"
    )
    assert validated["this_week"]["daily_plan"]["sunday"]["km"] <= 21.1
    assert validated["validation"]["long_run_enforced"] is True


def test_ai_engine_scenario6_validate_taper_enforcement():
    """S6 — Taper: week 3 before race has 32km → validated to ≤24km (75% of 32km max)."""
    from ppi.services.ai_coach_engine import AICoachEngine
    from datetime import date, timedelta

    engine = AICoachEngine()

    race_date = date.today() + timedelta(weeks=20)
    ctx = _make_ai_context(
        current_long_km=30.0,
        weeks_to_race=20,
        race_date=race_date.strftime("%Y-%m-%d"),
        goal_distance_km=42.195,
    )

    # Week that is exactly 3 weeks before race day
    week_3_before = race_date - timedelta(weeks=3)

    progression = [
        {
            "week_number": 17,
            "week_date":   week_3_before.strftime("%Y-%m-%d"),
            "target_km":   32.0,  # should be capped to 75% of max_long
            "phase":       "peak",
            "is_recovery_week": False,
            "is_peak_run": True,
            "label":       "Peak Run",
        }
    ]

    validated = engine._validate_progression(progression, ctx)

    # max_long = min(38, 42.195 * 0.90) = 37.97; 75% = 28.5
    # Since target was 32.0 and max_long is ~37.97, taper cap is 37.97 * 0.75 = 28.5
    assert len(validated) == 1
    assert validated[0]["target_km"] <= 29.0, (
        f"Expected ≤28.5km for taper week -3, got {validated[0]['target_km']}"
    )
    assert validated[0]["phase"] == "taper"
