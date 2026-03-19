"""
Intelligent compliance engine.

Compares last week planned vs actual, detects WHY sessions were missed,
and generates adaptive coaching response. Zero hardcoded values.

Field mapping (actual models):
  Activity.date          — DateTime (not start_date)
  Activity.activity_type — lowercase "run" (not sport_type)
  Activity.moving_time   — Float seconds
  WorkoutLog.workout_date       — Date
  WorkoutLog.target_distance_km — planned km
  WorkoutLog.session_name       — e.g. "Tempo Run"
"""
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..extensions import db
from ..models import Activity, Metric, WorkoutLog


class ComplianceEngine:
    """
    Compares last week planned vs actual Strava activities.
    Detects WHY sessions were missed.
    Generates adaptive coaching response.
    Works for any runner, any goal, any fitness level.
    """

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.today = date.today()
        self.result = self._compute()

    # ── Core computation ──────────────────────────────────────────────────────

    def _compute(self) -> dict:
        last_monday = self.today - timedelta(days=self.today.weekday() + 7)
        last_sunday = last_monday + timedelta(days=6)

        # ── Planned (WorkoutLog engine rows) ──
        planned_logs = WorkoutLog.query.filter(
            WorkoutLog.user_id == self.user_id,
            WorkoutLog.workout_date >= last_monday,
            WorkoutLog.workout_date <= last_sunday,
        ).all()

        # ── Actual (Strava activities last week) ──
        actual_activities = Activity.query.filter(
            Activity.user_id == self.user_id,
            Activity.date >= datetime.combine(last_monday, datetime.min.time()),
            Activity.date <= datetime.combine(last_sunday, datetime.max.time()),
        ).order_by(Activity.date.asc()).all()

        # ── Filter to run activities ──
        run_logs = [log for log in planned_logs if log.workout_type == "RUN"]
        run_activities = [
            act for act in actual_activities
            if (act.activity_type or "").lower() == "run"
        ]

        # ── Volume metrics ──
        planned_run_km = sum(log.target_distance_km or 0 for log in run_logs)
        planned_run_days = len(run_logs)
        actual_run_km = sum(act.distance_km or 0 for act in run_activities)
        actual_run_days = len({act.date.date() for act in run_activities})

        # ── Long run compliance ──
        planned_long = max((log.target_distance_km or 0) for log in run_logs) if run_logs else 0
        actual_long = max((act.distance_km or 0) for act in run_activities) if run_activities else 0
        long_run_done = (actual_long >= planned_long * 0.90) if planned_long > 0 else None

        # ── Quality session compliance (tempo / intervals) ──
        planned_quality = any(
            log.session_name in ("Tempo Run", "Interval Session")
            for log in planned_logs
        )
        actual_quality = any(
            (act.distance_km or 0) >= 6 and self._is_quality_effort(act)
            for act in run_activities
        )
        quality_done = actual_quality if planned_quality else None

        # ── Compliance percentages ──
        volume_compliance_pct = round(
            (actual_run_km / planned_run_km * 100) if planned_run_km > 0 else 0, 1
        )
        day_compliance_pct = round(
            (actual_run_days / planned_run_days * 100) if planned_run_days > 0 else 0, 1
        )

        # ── Gap detection ──
        max_gap = self._max_consecutive_rest_days(actual_activities, last_monday, last_sunday)

        # ── Load signals (ATL / TSB) ──
        load_signals = self._get_load_signals()

        # ── Why analysis ──
        miss_reason = self._detect_miss_reason(
            volume_compliance_pct=volume_compliance_pct,
            max_gap_days=max_gap,
            long_run_done=long_run_done,
            quality_done=quality_done,
            planned_quality=planned_quality,
            atl=load_signals["atl"],
            tsb=load_signals["tsb"],
            actual_run_days=actual_run_days,
            planned_run_days=planned_run_days,
        )

        # ── 4-week trend ──
        trend = self._compute_trend()

        # ── Adaptive coaching response ──
        response = self._generate_response(
            miss_reason=miss_reason,
            volume_compliance_pct=volume_compliance_pct,
            long_run_done=long_run_done,
            quality_done=quality_done,
            trend=trend,
            planned_run_km=planned_run_km,
            actual_run_km=actual_run_km,
        )

        return {
            "week_start":            last_monday.isoformat(),
            "week_end":              last_sunday.isoformat(),
            "planned_run_km":        round(planned_run_km, 1),
            "actual_run_km":         round(actual_run_km, 1),
            "planned_run_days":      planned_run_days,
            "actual_run_days":       actual_run_days,
            "planned_long_km":       round(planned_long, 1),
            "actual_long_km":        round(actual_long, 1),
            "long_run_done":         long_run_done,
            "quality_done":          quality_done,
            "volume_compliance_pct": volume_compliance_pct,
            "day_compliance_pct":    day_compliance_pct,
            "overall_status":        self._overall_status(volume_compliance_pct),
            "miss_reason":           miss_reason,
            "max_gap_days":          max_gap,
            "load_signals":          load_signals,
            "trend":                 trend,
            "response":              response,
            "this_week_adjustment":  response["volume_adjustment"],
        }

    # ── Miss reason detection ─────────────────────────────────────────────────

    def _detect_miss_reason(
        self, volume_compliance_pct, max_gap_days,
        long_run_done, quality_done, planned_quality,
        atl, tsb, actual_run_days, planned_run_days,
    ) -> dict:
        """
        Intelligently detect WHY sessions were missed.
        Returns reason code + confidence + explanation.

        Check order matters — more specific signals are evaluated first.
        complete_miss is checked before illness so zero-activity weeks
        are not misclassified as illness.
        """
        # 1. Perfect compliance
        if volume_compliance_pct >= 90:
            return {
                "code":        "on_track",
                "label":       "On Track",
                "confidence":  "high",
                "explanation": "Strong week — nearly all sessions completed.",
            }

        # 2. Fatigue-based miss — body needed extra rest
        if atl > 30 and tsb < -10 and volume_compliance_pct < 80:
            return {
                "code":        "fatigue",
                "label":       "Fatigue",
                "confidence":  "high",
                "explanation": (
                    f"Your fatigue load (ATL {atl:.0f}) was high and "
                    f"form (TSB {tsb:.0f}) was negative. "
                    "Your body likely needed the extra rest."
                ),
            }

        # 3. Complete miss — zero activity (checked before illness to
        #    avoid misrouting a planned rest week as illness)
        if actual_run_days == 0:
            return {
                "code":        "complete_miss",
                "label":       "Week Missed",
                "confidence":  "high",
                "explanation": (
                    "No runs recorded this week. "
                    "Whether planned rest or unexpected — "
                    "ease back in with short easy runs."
                ),
            }

        # 4. Illness or life event — 5+ consecutive days with no activity
        if max_gap_days >= 5:
            return {
                "code":        "illness_or_life",
                "label":       "Illness / Life Event",
                "confidence":  "high",
                "explanation": (
                    f"{max_gap_days} consecutive days with no activity. "
                    "Looks like illness or a significant life event. "
                    "Rebuild gently — do not try to make up lost runs."
                ),
            }

        # 5. Partial completion — some runs done, no fatigue signal
        if actual_run_days > 0 and 50 <= volume_compliance_pct < 90 and atl <= 30:
            return {
                "code":        "time_management",
                "label":       "Busy Week",
                "confidence":  "medium",
                "explanation": (
                    "You got some runs in but could not complete "
                    "the full plan. Life happens — "
                    "focus on key sessions this week."
                ),
            }

        # 6. Easy runs done but key sessions skipped
        if (
            actual_run_days >= planned_run_days * 0.6
            and not long_run_done
            and planned_quality
            and not quality_done
        ):
            return {
                "code":        "key_sessions_skipped",
                "label":       "Key Sessions Skipped",
                "confidence":  "medium",
                "explanation": (
                    "You completed easy runs but skipped the "
                    "long run and quality session. "
                    "These are the sessions that build marathon fitness."
                ),
            }

        # 7. Default: partial compliance, unclear reason
        return {
            "code":        "partial",
            "label":       "Partial Week",
            "confidence":  "low",
            "explanation": (
                f"Completed {volume_compliance_pct:.0f}% of "
                "planned volume. Keep building consistency."
            ),
        }

    # ── Adaptive coaching response ────────────────────────────────────────────

    def _generate_response(
        self, miss_reason, volume_compliance_pct,
        long_run_done, quality_done, trend,
        planned_run_km, actual_run_km,
    ) -> dict:
        """
        Generate adaptive coaching response.
        Determines this week's volume adjustment.
        Never punishes — always adapts intelligently.
        """
        code = miss_reason["code"]

        if code == "on_track":
            if trend["consecutive_good_weeks"] >= 3:
                return {
                    "message":            "Three strong weeks in a row — impressive consistency. Time to step up this week.",
                    "volume_adjustment":  1.10,
                    "long_run_adjustment": 1.0,
                    "priority":           "build",
                    "tone":               "affirming",
                }
            return {
                "message":            "Solid week — you hit your targets. Keep this momentum going.",
                "volume_adjustment":  1.05,
                "long_run_adjustment": 1.0,
                "priority":           "maintain",
                "tone":               "affirming",
            }

        if code == "fatigue":
            return {
                "message": (
                    "Your body was telling you something last week and you listened — "
                    "that is smart training. This week: reduce volume by 15%, "
                    "keep the long run but run it easy."
                ),
                "volume_adjustment":  0.85,
                "long_run_adjustment": 1.0,
                "priority":           "recovery",
                "tone":               "empathetic",
            }

        if code == "illness_or_life":
            return {
                "message": (
                    "Looks like a tough week — illness or life got in the way. "
                    "Do not try to make up what you missed. "
                    "Start fresh this week with easy runs, rebuild to 70% of normal volume."
                ),
                "volume_adjustment":  0.70,
                "long_run_adjustment": 0.80,
                "priority":           "rebuild",
                "tone":               "caring",
            }

        if code == "time_management":
            return {
                "message": (
                    "Busy week — it happens to every runner. "
                    "This week: protect your long run and quality session above everything else. "
                    "If time is short, skip the easy runs before skipping the key sessions."
                ),
                "volume_adjustment":  1.0,
                "long_run_adjustment": 1.0,
                "priority":           "key_sessions_first",
                "tone":               "practical",
            }

        if code == "key_sessions_skipped":
            return {
                "message": (
                    "Easy miles are good but they do not build marathon fitness alone. "
                    "This week: long run and tempo session are non-negotiable. "
                    "Schedule them first before anything else."
                ),
                "volume_adjustment":  1.0,
                "long_run_adjustment": 1.0,
                "priority":           "quality_focus",
                "tone":               "direct",
            }

        if code == "complete_miss":
            return {
                "message": (
                    "Fresh start this week. Do not chase last week — it is gone. "
                    "Begin with 2-3 easy runs, see how the body feels, "
                    "then decide on the long run."
                ),
                "volume_adjustment":  0.65,
                "long_run_adjustment": 0.75,
                "priority":           "gentle_restart",
                "tone":               "encouraging",
            }

        # Default partial
        return {
            "message": (
                f"You got {actual_run_km:.0f}km done out of {planned_run_km:.0f}km planned. "
                "Build on that this week — aim for at least your long run and one quality session."
            ),
            "volume_adjustment":  0.95,
            "long_run_adjustment": 1.0,
            "priority":           "consistency",
            "tone":               "neutral",
        }

    # ── Trend analysis ────────────────────────────────────────────────────────

    def _compute_trend(self) -> dict:
        """Analyze last 4 weeks compliance trend."""
        weekly_km = []
        for weeks_back in range(1, 5):
            monday = self.today - timedelta(days=self.today.weekday() + weeks_back * 7)
            sunday = monday + timedelta(days=6)
            km = (
                db.session.query(db.func.sum(Activity.distance_km))
                .filter(
                    Activity.user_id == self.user_id,
                    db.func.lower(Activity.activity_type) == "run",
                    Activity.date >= datetime.combine(monday, datetime.min.time()),
                    Activity.date <= datetime.combine(sunday, datetime.max.time()),
                )
                .scalar()
            ) or 0
            weekly_km.append(round(float(km), 1))

        # Consecutive weeks with meaningful training (≥20km)
        consecutive_good = 0
        for km in weekly_km:
            if km >= 20:
                consecutive_good += 1
            else:
                break

        # Trend direction — compare recent 2w average vs prior 2w average
        if len(weekly_km) >= 4:
            recent_avg = sum(weekly_km[:2]) / 2
            older_avg = sum(weekly_km[2:4]) / 2
            if recent_avg > older_avg * 1.05:
                direction = "improving"
            elif recent_avg < older_avg * 0.90:
                direction = "declining"
            else:
                direction = "steady"
        else:
            direction = "insufficient_data"

        return {
            "weekly_km_last_4":       weekly_km,
            "consecutive_good_weeks": consecutive_good,
            "direction":              direction,
            "avg_last_2_weeks":       round(sum(weekly_km[:2]) / 2, 1) if len(weekly_km) >= 2 else 0,
        }

    # ── Load signals ──────────────────────────────────────────────────────────

    def _get_load_signals(self) -> dict:
        """Get most recent ATL/TSB from Metric table."""
        try:
            metric = (
                Metric.query
                .filter_by(user_id=self.user_id)
                .order_by(Metric.date.desc())
                .first()
            )
            if metric:
                return {
                    "ctl": round(float(metric.ctl), 1),
                    "atl": round(float(metric.atl), 1),
                    "tsb": round(float(metric.tsb), 1),
                }
        except Exception:
            pass
        return {"ctl": 0, "atl": 0, "tsb": 0}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _max_consecutive_rest_days(self, activities, start: date, end: date) -> int:
        """Longest consecutive span of days with no activity in [start, end]."""
        active_days = {act.date.date() for act in activities}
        max_gap = current_gap = 0
        current = start
        while current <= end:
            if current not in active_days:
                current_gap += 1
                max_gap = max(max_gap, current_gap)
            else:
                current_gap = 0
            current += timedelta(days=1)
        return max_gap

    def _is_quality_effort(self, activity) -> bool:
        """Detect quality/hard effort by pace (< 6:30/km = 390 s/km)."""
        if not activity.moving_time or not activity.distance_km:
            return False
        pace_sec_per_km = activity.moving_time / activity.distance_km
        return pace_sec_per_km < 390

    def _overall_status(self, pct: float) -> str:
        if pct >= 90:  return "perfect"
        if pct >= 75:  return "good"
        if pct >= 50:  return "partial"
        if pct > 0:    return "missed"
        return "none"

    def to_dict(self) -> dict:
        return self.result
