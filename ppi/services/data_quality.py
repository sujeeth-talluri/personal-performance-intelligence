"""
Data quality gate for AI coaching.

Evaluates Strava data sufficiency before any coaching logic runs.
Uses Activity.date (DateTime) and Activity.activity_type (lowercase 'run').
"""
from datetime import date, datetime, timedelta

from ..extensions import db
from ..models import Activity, Goal


class DataQualityReport:
    """
    Evaluates Strava data sufficiency for AI coaching.
    Thresholds derived from goal context, not hardcoded.
    """

    MIN_WEEKS_FOR_AI = 4

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.today = date.today()
        self.report = self._evaluate()

    # ── Core evaluation ──────────────────────────────────────────────────────

    def _evaluate(self) -> dict:
        goal = Goal.query.filter_by(user_id=self.user_id).order_by(Goal.id.desc()).first()

        # All run activities, ordered by date ascending
        all_runs = (
            Activity.query
            .filter(
                Activity.user_id == self.user_id,
                db.func.lower(Activity.activity_type) == "run",
            )
            .order_by(Activity.date.asc())
            .all()
        )

        if not all_runs:
            return self._no_data_report(goal)

        # Date range of available data (Activity.date is DateTime)
        oldest = all_runs[0].date.date() if isinstance(all_runs[0].date, datetime) else all_runs[0].date
        newest = all_runs[-1].date.date() if isinstance(all_runs[-1].date, datetime) else all_runs[-1].date
        data_span_days  = (newest - oldest).days
        data_span_weeks = data_span_days / 7

        # Activities in last 4 weeks
        four_weeks_ago = datetime.combine(self.today - timedelta(weeks=4), datetime.min.time())
        recent_runs = [a for a in all_runs if a.date >= four_weeks_ago]

        # Distinct ISO weeks that have at least one run
        def _week_start(dt):
            d = dt.date() if isinstance(dt, datetime) else dt
            return (d - timedelta(days=d.weekday())).isoformat()

        recent_weeks_with_data = len({_week_start(a.date) for a in recent_runs})

        # Long runs (>= 15 km)
        long_runs        = [a for a in all_runs  if (a.distance_km or 0) >= 15]
        recent_long_runs = [a for a in long_runs if a.date >= four_weeks_ago]

        # Sufficiency: ≥ 4 weeks span AND ≥ 2 of last 4 weeks have a run
        is_sufficient = (
            data_span_weeks >= self.MIN_WEEKS_FOR_AI
            and recent_weeks_with_data >= 2
        )

        # Confidence tier
        if data_span_weeks >= 12 and recent_weeks_with_data >= 3:
            confidence       = "high"
            confidence_label = "High confidence"
        elif data_span_weeks >= 8 and recent_weeks_with_data >= 2:
            confidence       = "medium"
            confidence_label = "Moderate confidence"
        elif data_span_weeks >= 4 and recent_weeks_with_data >= 2:
            confidence       = "low"
            confidence_label = "Low confidence — more data improves accuracy"
        else:
            confidence       = "insufficient"
            confidence_label = "Not enough data yet"

        # Specific gaps
        missing = []
        if data_span_weeks < self.MIN_WEEKS_FOR_AI:
            weeks_needed = self.MIN_WEEKS_FOR_AI - data_span_weeks
            missing.append(f"{weeks_needed:.0f} more week(s) of training data needed")
        if recent_weeks_with_data < 2:
            missing.append("Recent training data needed — sync your latest runs")
        if not long_runs:
            missing.append("No long runs detected — log a run of 15 km+ for better coaching")

        banner = self._build_banner(confidence, data_span_weeks, recent_weeks_with_data, missing, goal)

        return {
            "is_sufficient":          is_sufficient,
            "confidence":             confidence,
            "confidence_label":       confidence_label,
            "data_span_weeks":        round(data_span_weeks, 1),
            "total_runs":             len(all_runs),
            "recent_runs_count":      len(recent_runs),
            "recent_weeks_with_data": recent_weeks_with_data,
            "long_runs_count":        len(long_runs),
            "recent_long_runs_count": len(recent_long_runs),
            "oldest_activity":        oldest.isoformat(),
            "newest_activity":        newest.isoformat(),
            "missing":                missing,
            "banner":                 banner,
            "show_banner":            confidence != "high",
        }

    def _no_data_report(self, goal) -> dict:
        return {
            "is_sufficient":          False,
            "confidence":             "no_data",
            "confidence_label":       "No data",
            "data_span_weeks":        0,
            "total_runs":             0,
            "recent_runs_count":      0,
            "recent_weeks_with_data": 0,
            "long_runs_count":        0,
            "recent_long_runs_count": 0,
            "oldest_activity":        None,
            "newest_activity":        None,
            "missing":                ["No Strava activities found — sync your runs"],
            "banner": {
                "type":       "warning",
                "icon":       "📊",
                "message":    (
                    "No training data found. "
                    "Sync your Strava activities to unlock your personalized coaching plan."
                ),
                "action":     "Sync Strava Now",
                "action_url": "/?sync=1",
            },
            "show_banner": True,
        }

    def _build_banner(self, confidence, span_weeks, recent_weeks, missing, goal) -> dict:
        goal_label = f"{goal.race_name} ({goal.goal_time})" if goal else "your goal race"

        if confidence == "insufficient":
            return {
                "type":    "warning",
                "icon":    "📊",
                "message": (
                    f"Your coaching plan for {goal_label} needs more data. "
                    f"We have {span_weeks:.0f} week(s) of your training — "
                    f"we need at least {self.MIN_WEEKS_FOR_AI}. "
                    f"Sync Strava to add your recent runs."
                ),
                "action":     "Sync Strava",
                "action_url": "/?sync=1",
            }

        if confidence == "low":
            return {
                "type":    "info",
                "icon":    "💡",
                "message": (
                    f"Coaching plan generated with {span_weeks:.0f} weeks of data — "
                    f"accuracy improves as you log more runs. "
                    f"Keep training consistently for sharper insights."
                ),
                "action":     None,
                "action_url": None,
            }

        if confidence == "medium":
            return {
                "type":    "info",
                "icon":    "📈",
                "message": (
                    f"Good data foundation — {span_weeks:.0f} weeks of training history. "
                    f"Coaching accuracy improves week by week as you log more runs."
                ),
                "action":     None,
                "action_url": None,
            }

        # High confidence — no banner needed
        return {
            "type":       "success",
            "icon":       "✅",
            "message":    "",
            "action":     None,
            "action_url": None,
        }

    # ── Public accessors ─────────────────────────────────────────────────────

    @property
    def is_sufficient(self) -> bool:
        return self.report["is_sufficient"]

    @property
    def banner(self) -> dict:
        return self.report["banner"]

    @property
    def show_banner(self) -> bool:
        return self.report["show_banner"]

    @property
    def confidence(self) -> str:
        return self.report["confidence"]

    def to_dict(self) -> dict:
        return self.report
