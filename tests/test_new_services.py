"""Tests for ppi.services.future_plan_service and ppi.services.current_week_service."""
import pytest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

from ppi.services.future_plan_service import (
    _ADAPTIVE_GOAL_BANDS,
    _get_adaptive_goal_band,
    _display_planned_km,
)
from ppi.services.current_week_service import (
    _secs_to_pace_str,
    _pace_guidance_for_session,
    _status_label,
    _schedule_preferences_from_profile,
    _week_bounds,
    fix_coaching_numbers,
    _build_session_verdict,
    _parse_iso_datetime,
    _should_sync_now,
    _different_activity_status_label,
)


# ---------------------------------------------------------------------------
# future_plan_service tests
# ---------------------------------------------------------------------------

def test_adaptive_goal_band_sub3():
    # 2:45:00 = 9900 seconds — sub-3:00 band (< 10800)
    band = _get_adaptive_goal_band(9900)
    assert band["min_km"] == 70
    assert band["max_km"] == 120


def test_adaptive_goal_band_sub4():
    # 3:45:00 = 13500 seconds — sub-4:00 band (< 14400)
    band = _get_adaptive_goal_band(13500)
    assert band["min_km"] == 42
    assert band["max_km"] == 75


def test_adaptive_goal_band_slow():
    # 6:00:00 = 21600 seconds — 5:00+ band (> 18000)
    band = _get_adaptive_goal_band(21600)
    assert band["min_km"] == 20
    assert band["max_km"] == 42


def test_adaptive_goal_band_boundary_sub330():
    # 12599 seconds — just under 12600 = 3:30:00 boundary, so sub-3:30 band (< 12600)
    band = _get_adaptive_goal_band(12599)
    assert band["min_km"] == 55
    assert band["max_km"] == 95


def test_display_planned_km_rounds_to_int():
    assert _display_planned_km(14.6) == 15
    assert _display_planned_km(14.4) == 14


def test_display_planned_km_zero_for_none():
    assert _display_planned_km(None) == 0
    assert _display_planned_km(0) == 0


def test_adaptive_goal_bands_table_sorted_ascending():
    # Each band's max_seconds should be strictly greater than the previous
    for i in range(1, len(_ADAPTIVE_GOAL_BANDS)):
        prev_max = _ADAPTIVE_GOAL_BANDS[i - 1][0]
        curr_max = _ADAPTIVE_GOAL_BANDS[i][0]
        assert curr_max > prev_max, (
            f"Band at index {i} has max_seconds={curr_max} which is not > "
            f"previous {prev_max}"
        )


def test_adaptive_goal_band_returns_fallback_for_extreme_slow():
    band = _get_adaptive_goal_band(99999)
    assert "min_km" in band
    assert "max_km" in band
    assert "lr_cap" in band
    assert "required_km" in band


# ---------------------------------------------------------------------------
# current_week_service tests
# ---------------------------------------------------------------------------

def test_secs_to_pace_str_formats_correctly():
    assert _secs_to_pace_str(360) == "6:00/km"
    assert _secs_to_pace_str(330) == "5:30/km"
    assert _secs_to_pace_str(263) == "4:23/km"


def test_pace_guidance_for_session_uses_descriptive_when_no_pace():
    result = _pace_guidance_for_session("Long Run")
    assert isinstance(result, str) and len(result) > 0
    result_unknown = _pace_guidance_for_session("Unknown Session")
    assert result_unknown == ""


def test_pace_guidance_for_session_computes_ranges_from_goal_pace():
    result = _pace_guidance_for_session("Easy Run", 360)
    assert "/" in result
    assert "km" in result
    assert "–" in result
    # Should contain two pace values separated by em-dash
    parts = result.split("–")
    assert len(parts) == 2


def test_status_label_maps_known_statuses():
    assert _status_label("completed") == "Completed"
    assert _status_label("missed") == "Missed"
    assert _status_label("skipped") == "Missed"
    assert _status_label("planned") == "Planned"
    assert _status_label("moved") == "Moved"


def test_status_label_titlecases_unknown():
    assert _status_label("partial") == "Partial"
    assert _status_label("overdone") == "Overdone"


def test_schedule_preferences_returns_defaults_for_none():
    prefs = _schedule_preferences_from_profile(None)
    assert prefs["training_days_per_week"] == 5
    assert prefs["long_run_day"] == "sunday"
    assert prefs["strength_days_per_week"] == 2


def test_schedule_preferences_reads_profile_attributes():
    profile = MagicMock()
    profile.training_days_per_week = 4
    profile.long_run_day = "Saturday"
    profile.strength_days_per_week = 1
    prefs = _schedule_preferences_from_profile(profile)
    assert prefs["training_days_per_week"] == 4
    assert prefs["long_run_day"] == "saturday"


def test_week_bounds_monday_start():
    # 2026-03-25 is a Wednesday
    week_start, week_end = _week_bounds(date(2026, 3, 25))
    assert week_start == date(2026, 3, 23)  # Monday
    assert week_end == date(2026, 3, 29)    # Sunday


def test_week_bounds_on_monday():
    # 2026-03-23 is a Monday
    week_start, week_end = _week_bounds(date(2026, 3, 23))
    assert week_start == date(2026, 3, 23)
    assert week_end == date(2026, 3, 29)


def test_fix_coaching_numbers_replaces_weekly_range():
    result = fix_coaching_numbers("Target 25-28km this week", 42.0, 18.0)
    assert "25" not in result
    assert "28" not in result
    assert "42" in result


def test_fix_coaching_numbers_replaces_long_run():
    result = fix_coaching_numbers("long run of 14km is key", 42.0, 18.0)
    assert "18" in result
    assert "14km" not in result or "14km on the long" not in result


def test_fix_coaching_numbers_leaves_unmatched_text_alone():
    msg = "Great training session!"
    result = fix_coaching_numbers(msg, 42.0, 18.0)
    assert result == msg


def test_build_session_verdict_nailed_target():
    item = {"done": True, "actual_km": 10.0, "planned_km": 10.0, "workout_type": "RUN"}
    verdict = _build_session_verdict(item)
    assert verdict is not None
    assert "✓" in verdict
    assert "10.0" in verdict


def test_build_session_verdict_over_target():
    item = {"done": True, "actual_km": 12.0, "planned_km": 10.0, "workout_type": "RUN"}
    verdict = _build_session_verdict(item)
    assert verdict is not None
    assert "⚡" in verdict
    assert "2.0" in verdict


def test_build_session_verdict_none_when_not_done():
    item = {"done": False, "actual_km": 10.0, "planned_km": 10.0, "workout_type": "RUN"}
    verdict = _build_session_verdict(item)
    assert verdict is None


def test_build_session_verdict_gym_completed():
    item = {"done": True, "workout_type": "STRENGTH", "actual_km": 0.0, "planned_km": 0.0}
    verdict = _build_session_verdict(item)
    assert verdict == "Gym session completed ✓"


def test_parse_iso_datetime_valid():
    result = _parse_iso_datetime("2026-03-29T10:30:00")
    assert isinstance(result, datetime)
    assert _parse_iso_datetime(None) is None


def test_parse_iso_datetime_invalid():
    assert _parse_iso_datetime("not-a-date") is None


def test_should_sync_now_when_no_last_sync():
    assert _should_sync_now(None, 15) is True


def test_should_sync_now_within_cooldown():
    last_sync_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    assert _should_sync_now(last_sync_at, 15) is False


def test_should_sync_now_after_cooldown():
    last_sync_at = datetime.now(timezone.utc) - timedelta(minutes=20)
    assert _should_sync_now(last_sync_at, 15) is True


def test_different_activity_status_label_run():
    item = {"workout_type": "RUN"}
    assert _different_activity_status_label(item) == "Run missed"
    assert _different_activity_status_label(item, is_today=True) == "Run open"


def test_different_activity_status_label_strength():
    item = {"workout_type": "STRENGTH"}
    assert _different_activity_status_label(item) == "Gym missed"
    assert _different_activity_status_label(item, is_today=True) == "Gym open"
