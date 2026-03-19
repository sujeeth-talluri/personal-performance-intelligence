"""
VDOT-based goal feasibility engine.

Works for any runner, any goal distance, any target time.
Honest assessment — suggests revised goal only when feasibility < 50%.

Field notes:
  Goal.race_distance  — goal distance in km (NOT distance_km)
  Activity.date       — DateTime (comparisons require datetime.combine)
  Activity.moving_time — Float seconds
  Activity.activity_type — lowercase string, e.g. "run"
"""
import math
from collections import defaultdict
from datetime import date, datetime, timedelta

from ..extensions import db
from ..models import Activity, Goal


class FeasibilityEngine:
    """
    VDOT-based goal feasibility assessment.
    Honest assessment — suggests revised goal only if score < 50%.
    """

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.today = date.today()
        self.result = self._assess()

    # ── VDOT core math ────────────────────────────────────────────────────────

    def _vdot_from_race(self, distance_km: float, time_seconds: int) -> float:
        """Compute VDOT from a race result (Jack Daniels formula)."""
        if not distance_km or not time_seconds:
            return 0.0
        t = time_seconds / 60.0       # minutes
        d = distance_km * 1000        # metres
        velocity = d / t              # m/min

        pct_max = (
            0.8
            + 0.1894393 * math.exp(-0.012778 * t)
            + 0.2989558 * math.exp(-0.1932605 * t)
        )
        vo2 = -4.6 + 0.182258 * velocity + 0.000104 * velocity ** 2
        return round(vo2 / pct_max, 2)

    def _vdot_to_race_time(self, vdot: float, distance_km: float) -> int:
        """Predict race time (seconds) from VDOT and distance (binary search)."""
        if not vdot or not distance_km:
            return 0
        lo, hi = 600, 86400
        for _ in range(50):
            mid = (lo + hi) / 2
            if self._vdot_from_race(distance_km, mid) > vdot:
                lo = mid
            else:
                hi = mid
        return int((lo + hi) / 2)

    def _required_vdot(self, goal_seconds: int, distance_km: float) -> float:
        return self._vdot_from_race(distance_km, goal_seconds)

    # ── Current VDOT estimation ───────────────────────────────────────────────

    def _estimate_current_vdot(self, goal, activities: list) -> dict:
        """
        Estimate current VDOT from PBs, recent races, and quality training runs.
        Takes the highest VDOT as the current fitness ceiling.
        """
        vdot_sources = []

        # Source 1: Stored PBs on goal
        pb_map = {
            "5.0":  (5.0,    goal.pb_5k),
            "10.0": (10.0,   goal.pb_10k),
            "21.1": (21.097, goal.pb_hm),
            "42.2": (42.195, goal.personal_best),
        }
        for key, (dist_km, pb_time) in pb_map.items():
            if pb_time:
                secs = self._parse_time(pb_time)
                if secs:
                    v = self._vdot_from_race(dist_km, secs)
                    if v > 20:
                        vdot_sources.append({
                            "vdot": v,
                            "source": f"PB {key}km ({pb_time})",
                            "recency_weight": 0.85,
                        })

        # Source 2: Recent race activities (last 180 days)
        cutoff_180 = datetime.combine(self.today - timedelta(days=180), datetime.min.time())
        races = [
            a for a in activities
            if a.activity_type
            and "race" in a.activity_type.lower()
            and a.date >= cutoff_180
            and (a.distance_km or 0) >= 5
        ]
        for race in races:
            if race.distance_km and race.moving_time:
                v = self._vdot_from_race(race.distance_km, race.moving_time)
                if v > 20:
                    days_ago = (self.today - race.date.date()).days
                    recency = max(0.7, 1.0 - days_ago / 365)
                    vdot_sources.append({
                        "vdot": v,
                        "source": f"Race {race.distance_km:.1f}km",
                        "recency_weight": recency,
                    })

        # Source 3: Recent quality training runs (last 60 days, pace < 390 s/km)
        cutoff_60 = datetime.combine(self.today - timedelta(days=60), datetime.min.time())
        quality_runs = [
            a for a in activities
            if a.date >= cutoff_60
            and (a.distance_km or 0) >= 8
            and a.moving_time
            and self._pace_sec_per_km(a) < 390
        ]
        for run in quality_runs[:3]:
            # Apply 1.04 factor — training runs are harder than race efforts at same pace
            adjusted_time = int(run.moving_time * 1.04)
            v = self._vdot_from_race(run.distance_km, adjusted_time)
            if v > 20:
                vdot_sources.append({
                    "vdot": v,
                    "source": f"Training {run.distance_km:.1f}km",
                    "recency_weight": 0.90,
                })

        if not vdot_sources:
            return {
                "vdot": 0,
                "source": "insufficient_data",
                "confidence": "none",
                "all_sources": [],
            }

        best = max(vdot_sources, key=lambda x: x["vdot"])
        weighted = sum(s["vdot"] * s["recency_weight"] for s in vdot_sources) / sum(
            s["recency_weight"] for s in vdot_sources
        )
        confidence = (
            "high"   if len(vdot_sources) >= 3 else
            "medium" if len(vdot_sources) >= 2 else
            "low"
        )
        return {
            "vdot":          best["vdot"],
            "weighted_vdot": round(weighted, 2),
            "source":        best["source"],
            "confidence":    confidence,
            "all_sources":   vdot_sources,
        }

    # ── Training readiness ────────────────────────────────────────────────────

    def _training_readiness(self, goal, activities: list, weeks_to_race: float) -> dict:
        """
        Assess volume and long-run readiness relative to goal requirements.
        All thresholds scale automatically from goal pace.
        """
        goal_seconds = self._parse_time(goal.goal_time)
        goal_km = float(goal.race_distance or 42.195)

        if not goal_seconds:
            return {}

        goal_pace = goal_seconds / goal_km  # sec/km

        # Required weekly peak volume — linear interpolation between pace anchors
        # 4:00/km pace needs ~95km peak; 8:00/km needs ~35km peak
        pace_fast, vol_at_fast = 240, 95
        pace_slow, vol_at_slow = 480, 35
        required_peak_weekly = vol_at_fast + (
            (goal_pace - pace_fast) / (pace_slow - pace_fast) * (vol_at_slow - vol_at_fast)
        )
        required_peak_weekly = round(max(35, min(100, required_peak_weekly)), 1)

        # Required long run (85% of race distance, capped at 38km)
        required_long_run = round(min(38.0, goal_km * 0.85), 1)

        # Current metrics — last 4 weeks
        four_weeks_ago = datetime.combine(self.today - timedelta(weeks=4), datetime.min.time())
        recent = [
            a for a in activities
            if a.date >= four_weeks_ago
            and a.activity_type
            and "run" in a.activity_type.lower()
        ]

        weekly_vols = self._weekly_volumes(activities, 4)
        recent_avg = sum(weekly_vols) / len(weekly_vols) if weekly_vols else 0
        current_long = max((a.distance_km or 0) for a in recent) if recent else 0

        # Gaps
        volume_gap = max(0, required_peak_weekly - recent_avg)
        long_run_gap = max(0, required_long_run - current_long)

        # Weeks to close volume gap (10% rule)
        weeks_to_close_volume = 0
        if volume_gap > 0 and recent_avg > 0:
            v, w = recent_avg, 0
            while v < required_peak_weekly and w < 52:
                v *= 1.10
                w += 1
            weeks_to_close_volume = w

        # Weeks to close long run gap (2km per fortnight)
        weeks_to_close_long = math.ceil(long_run_gap / 2) * 2 if long_run_gap > 0 else 0

        weeks_needed = max(weeks_to_close_volume, weeks_to_close_long) + 3  # +3 for taper

        return {
            "required_peak_weekly_km": required_peak_weekly,
            "required_long_run_km":    required_long_run,
            "current_weekly_avg_km":   round(recent_avg, 1),
            "current_long_run_km":     round(current_long, 1),
            "volume_gap_km":           round(volume_gap, 1),
            "long_run_gap_km":         round(long_run_gap, 1),
            "weeks_to_close_volume":   weeks_to_close_volume,
            "weeks_to_close_long":     weeks_to_close_long,
            "weeks_needed_total":      round(weeks_needed, 1),
            "time_sufficient":         weeks_to_race >= weeks_needed,
        }

    # ── VDOT improvement projection ───────────────────────────────────────────

    def _vdot_projection(
        self, current_vdot: float, required_vdot: float,
        weeks_to_race: float, consistency_score: float,
    ) -> dict:
        """
        Project whether VDOT can reach required level by race day.
        Improvement rate: 0.3–1.5 VDOT points/month, scales with fitness level and consistency.
        """
        vdot_gap = max(0, required_vdot - current_vdot)

        if vdot_gap <= 0:
            return {
                "gap":                        0,
                "monthly_improvement_rate":   0,
                "months_needed":              0,
                "months_available":           round(weeks_to_race / 4.33, 1),
                "achievable":                 True,
                "projected_vdot_race_day":    current_vdot,
            }

        # Higher VDOT = harder to improve; better consistency = faster improvement
        base_rate = max(0.3, 1.5 - (current_vdot - 30) * 0.03)
        consistency_multiplier = 0.5 + (consistency_score / 100)
        monthly_rate = round(max(0.2, min(1.5, base_rate * consistency_multiplier)), 2)

        months_available = weeks_to_race / 4.33
        months_needed = vdot_gap / monthly_rate
        projected_vdot = round(current_vdot + (monthly_rate * months_available), 2)

        return {
            "gap":                      round(vdot_gap, 2),
            "monthly_improvement_rate": monthly_rate,
            "months_needed":            round(months_needed, 1),
            "months_available":         round(months_available, 1),
            "achievable":               months_available >= months_needed,
            "projected_vdot_race_day":  projected_vdot,
        }

    # ── Consistency score ─────────────────────────────────────────────────────

    def _consistency_score(self, activities: list) -> float:
        """Score 0–100 based on training consistency over last 8 weeks."""
        weekly_vols = self._weekly_volumes(activities, 8)
        if not weekly_vols:
            return 0.0

        active_weeks = sum(1 for v in weekly_vols if v >= 20)
        base_score = (active_weeks / 8) * 100

        if len(weekly_vols) >= 2:
            avg = sum(weekly_vols) / len(weekly_vols)
            variance = sum((v - avg) ** 2 for v in weekly_vols) / len(weekly_vols)
            cv = (variance ** 0.5) / avg if avg > 0 else 1
            base_score = max(0, base_score - min(20, cv * 20))

        return round(base_score, 1)

    # ── Revised goal suggestion ───────────────────────────────────────────────

    def _suggest_revised_goal(
        self, current_vdot: float, vdot_projection: dict,
        goal_km: float, weeks_to_race: float,
    ) -> dict | None:
        """
        Suggest a realistic revised goal based on projected VDOT at race day.
        Only called when feasibility < 50%.
        """
        projected_vdot = vdot_projection["projected_vdot_race_day"]
        race_vdot = projected_vdot * 0.97  # 3% race-day buffer

        predicted_seconds = self._vdot_to_race_time(race_vdot, goal_km)
        if not predicted_seconds:
            return None

        rounded = round(predicted_seconds / 300) * 300  # nearest 5 minutes
        h = rounded // 3600
        m = (rounded % 3600) // 60
        revised_time = f"{h}:{m:02d}:00"

        return {
            "revised_goal_time":        revised_time,
            "revised_goal_seconds":     rounded,
            "based_on_projected_vdot":  round(race_vdot, 2),
            "reasoning": (
                f"Based on your current VDOT ({current_vdot:.1f}) "
                f"and projected improvement over {weeks_to_race:.0f} weeks, "
                f"{revised_time} is a more achievable target."
            ),
        }

    # ── Main assessment ───────────────────────────────────────────────────────

    def _assess(self) -> dict:
        goal = Goal.query.filter_by(user_id=self.user_id).order_by(Goal.id.desc()).first()

        if not goal or not goal.race_date:
            return self._empty_result()

        weeks_to_race = round((goal.race_date - self.today).days / 7, 1)
        goal_seconds = self._parse_time(goal.goal_time)
        goal_km = float(goal.race_distance or 42.195)

        if not goal_seconds:
            return self._empty_result()

        # Activities last 6 months
        cutoff = datetime.combine(self.today - timedelta(days=180), datetime.min.time())
        activities = Activity.query.filter(
            Activity.user_id == self.user_id,
            Activity.date >= cutoff,
        ).order_by(Activity.date.asc()).all()

        # Core computations
        required_vdot = self._required_vdot(goal_seconds, goal_km)
        current_vdot_data = self._estimate_current_vdot(goal, activities)
        current_vdot = current_vdot_data["vdot"]
        consistency = self._consistency_score(activities)
        readiness = self._training_readiness(goal, activities, weeks_to_race)
        projection = self._vdot_projection(current_vdot, required_vdot, weeks_to_race, consistency)

        # ── Feasibility score (0–100) — 4 weighted factors ──

        # Factor 1: VDOT gap (35%) — aerobic capability
        vdot_score = min(100, (current_vdot / required_vdot) * 100) if required_vdot > 0 else 0

        # Factor 2: Time available (25%) — weeks vs weeks needed
        weeks_needed = readiness.get("weeks_needed_total", 20) if readiness else 20
        time_score = min(100, (weeks_to_race / max(1, weeks_needed)) * 100)

        # Factor 3: Volume readiness (25%) — current vs required weekly km
        req_vol = readiness.get("required_peak_weekly_km", 55) if readiness else 55
        curr_vol = readiness.get("current_weekly_avg_km", 0) if readiness else 0
        volume_score = min(100, (curr_vol / req_vol) * 100) if req_vol > 0 else 0

        # Factor 4: Consistency (15%)
        feasibility_score = round(
            vdot_score * 0.35
            + time_score * 0.25
            + volume_score * 0.25
            + consistency * 0.15,
            1,
        )

        # Assessment label
        if feasibility_score >= 75:
            assessment, label, color = "on_track",       "On Track",                      "green"
        elif feasibility_score >= 60:
            assessment, label, color = "achievable",     "Achievable with Consistency",    "amber"
        elif feasibility_score >= 50:
            assessment, label, color = "at_risk",        "At Risk — Needs Focus",          "orange"
        else:
            assessment, label, color = "needs_revision", "Goal Needs Revision",            "red"

        # Revised goal — only when feasibility < 50%
        revised_goal = None
        if feasibility_score < 50 and current_vdot > 0:
            revised_goal = self._suggest_revised_goal(
                current_vdot, projection, goal_km, weeks_to_race
            )

        requirements = self._build_requirements(
            vdot_score, time_score, volume_score, consistency, readiness, projection, weeks_to_race
        )
        honest_text = self._build_honest_text(
            assessment, current_vdot, required_vdot, readiness, projection, weeks_to_race, goal, feasibility_score
        )

        return {
            "goal_time":       goal.goal_time,
            "goal_distance_km": goal_km,
            "weeks_to_race":   weeks_to_race,

            "vdot": {
                "required":        round(required_vdot, 2),
                "current":         current_vdot,
                "source":          current_vdot_data["source"],
                "confidence":      current_vdot_data["confidence"],
                "gap":             round(max(0, required_vdot - current_vdot), 2),
                "already_capable": current_vdot >= required_vdot,
            },

            "feasibility_score":  feasibility_score,
            "assessment":         assessment,
            "assessment_label":   label,
            "assessment_color":   color,

            "factor_scores": {
                "vdot":        round(vdot_score, 1),
                "time":        round(time_score, 1),
                "volume":      round(volume_score, 1),
                "consistency": round(consistency, 1),
            },

            "readiness":          readiness,
            "projection":         projection,
            "consistency_score":  consistency,
            "requirements":       requirements,
            "honest_assessment":  honest_text,

            "revised_goal":       revised_goal,
            "show_revised_goal":  feasibility_score < 50,
        }

    # ── Text builders ─────────────────────────────────────────────────────────

    def _build_requirements(
        self, vdot_score, time_score, volume_score,
        consistency, readiness, projection, weeks_to_race,
    ) -> list:
        reqs = []
        if vdot_score < 85:
            reqs.append(
                f"Improve aerobic fitness — VDOT needs {projection.get('gap', 0):.1f} more points"
            )
        if volume_score < 70:
            gap = readiness.get("volume_gap_km", 0) if readiness else 0
            reqs.append(
                f"Build weekly volume by {gap:.0f}km/week "
                f"(10% rule — takes {readiness.get('weeks_to_close_volume', 0)} weeks)"
            )
        if readiness and readiness.get("long_run_gap_km", 0) > 3:
            gap = readiness["long_run_gap_km"]
            reqs.append(
                f"Extend long run by {gap:.0f}km "
                f"({readiness.get('weeks_to_close_long', 0)} weeks at 2km/fortnight)"
            )
        if consistency < 60:
            reqs.append("Improve consistency — aim for 4+ training days every week")
        if time_score < 70:
            reqs.append(
                f"Limited time — {weeks_to_race:.0f} weeks available, every week counts now"
            )
        if not reqs:
            reqs.append("Stay healthy and consistent — you have what it takes")
        return reqs

    def _build_honest_text(
        self, assessment, current_vdot, required_vdot,
        readiness, projection, weeks_to_race, goal, score,
    ) -> str:
        goal_label = goal.goal_time or "your goal"
        curr_long = readiness.get("current_long_run_km", 0) if readiness else 0
        req_long  = readiness.get("required_long_run_km", 0) if readiness else 0
        curr_vol  = readiness.get("current_weekly_avg_km", 0) if readiness else 0
        req_vol   = readiness.get("required_peak_weekly_km", 0) if readiness else 0

        if assessment == "on_track":
            return (
                f"Your VDOT of {current_vdot:.1f} already meets "
                f"the {required_vdot:.1f} required for {goal_label}. "
                f"At {curr_vol:.0f}km/week and a {curr_long:.0f}km long run, "
                f"your training is heading in the right direction. "
                f"Stay consistent for {weeks_to_race:.0f} more weeks and you will be ready."
            )
        elif assessment == "achievable":
            capable_str = (
                "already sufficient"
                if current_vdot >= required_vdot
                else f"close to the {required_vdot:.1f} required"
            )
            return (
                f"Your aerobic base (VDOT {current_vdot:.1f}) is {capable_str}. "
                f"The main work is building your long run from {curr_long:.0f}km to {req_long:.0f}km "
                f"and weekly volume from {curr_vol:.0f}km to {req_vol:.0f}km. "
                f"With {weeks_to_race:.0f} weeks available and consistent training, "
                f"{goal_label} is within reach."
            )
        elif assessment == "at_risk":
            return (
                f"{goal_label} is possible but will require near-perfect execution from here. "
                f"You need {req_vol:.0f}km weeks and {req_long:.0f}km long runs — "
                f"currently at {curr_vol:.0f}km and {curr_long:.0f}km. "
                f"Any significant training gaps in the next {weeks_to_race:.0f} weeks "
                f"will make this very difficult."
            )
        else:
            return (
                f"Based on current training ({curr_vol:.0f}km/week, {curr_long:.0f}km long run) "
                f"and {weeks_to_race:.0f} weeks to race day, {goal_label} is unlikely "
                f"without a significant step-up. "
                f"A revised goal is shown below based on your realistic trajectory."
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _weekly_volumes(self, activities: list, weeks: int) -> list:
        """Return list of weekly km totals for the last N weeks."""
        weekly: dict = defaultdict(float)
        cutoff = datetime.combine(self.today - timedelta(weeks=weeks), datetime.min.time())
        for a in activities:
            if a.date < cutoff:
                continue
            if not (a.activity_type and "run" in a.activity_type.lower()):
                continue
            # Monday of this activity's week
            wk = a.date - timedelta(days=a.date.weekday())
            weekly[wk] += a.distance_km or 0
        return [round(v, 1) for v in weekly.values()]

    def _pace_sec_per_km(self, activity) -> float:
        if not activity.moving_time or not activity.distance_km:
            return 999.0
        return activity.moving_time / activity.distance_km

    def _parse_time(self, time_str: str) -> int | None:
        if not time_str:
            return None
        try:
            parts = str(time_str).strip().split(":")
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
        except Exception:
            return None

    def _empty_result(self) -> dict:
        return {
            "feasibility_score": 0,
            "assessment":        "unknown",
            "assessment_label":  "Set a goal to begin",
            "assessment_color":  "grey",
            "vdot": {
                "required": 0, "current": 0,
                "gap": 0, "already_capable": False,
                "source": "none", "confidence": "none",
            },
            "honest_assessment": "Set a race goal and date to unlock your feasibility assessment.",
            "revised_goal":      None,
            "show_revised_goal": False,
            "requirements":      [],
            "readiness":         {},
            "projection":        {},
            "consistency_score": 0,
            "factor_scores":     {"vdot": 0, "time": 0, "volume": 0, "consistency": 0},
        }

    def to_dict(self) -> dict:
        return self.result
