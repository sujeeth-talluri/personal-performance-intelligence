"""
AI Coach Engine — orchestrates all coaching engines and calls Claude
to generate a personalized weekly training plan.

Architecture:
  1. Check smart cache (Step 7)
  2. Gather all engine outputs (DQ + Compliance + Feasibility)
  3. Build runner context
  4. Call Claude AI — Call A: phase + weekly plan (Step 5)
  5. Call Claude AI — Call B: long run progression (Step 5)
  6. Validate AI output (Step 6)
  7. Store in cache (Step 7)
  8. Return canonical plan

Field notes:
  goal.race_distance  — goal distance in km (NOT distance_km)
  Activity.date       — DateTime; use datetime.combine for comparisons
"""
import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests
from flask import current_app

from ..extensions import db
from ..models import Activity, CoachingPlan, Goal, RunnerProfile
from .compliance_engine import ComplianceEngine
from .data_quality import DataQualityReport
from .feasibility_engine import FeasibilityEngine


class AICoachEngine:
    """
    Orchestrates all coaching engines and calls Claude AI
    to generate a personalized weekly training plan.
    """

    def get_plan(self, user_id: int, force_refresh: bool = False) -> dict:
        """
        Main entry point. Returns coaching plan.
        Uses cache unless stale or force_refresh=True.
        """
        # STEP 7A: Check cache
        if not force_refresh:
            cached = self._get_cached_plan(user_id)
            if cached:
                return cached

        # Gather all context
        context = self._build_full_context(user_id)
        if not context:
            return self._fallback_plan("No goal set", user_id)

        # STEP 5A: AI Call — phase + this week
        try:
            weekly_plan = self._call_claude_weekly(context)
        except Exception as e:
            current_app.logger.warning(f"AI weekly call failed: {e}")
            weekly_plan = self._fallback_weekly(context)

        # STEP 5B: AI Call — long run progression
        try:
            progression = self._call_claude_progression(context, weekly_plan)
        except Exception as e:
            current_app.logger.warning(f"AI progression call failed: {e}")
            progression = self._fallback_progression(context)

        # STEP 6: Validate both outputs
        weekly_plan = self._validate_weekly(weekly_plan, context)
        progression = self._validate_progression(progression, context)

        # Assemble final plan
        plan = {
            **weekly_plan,
            "long_run_progression": progression,
            "feasibility":          context["feasibility"],
            "compliance":           context["compliance"],
            "data_quality":         context["data_quality"],
            "runner_profile":       context["runner_profile"],
            "generated_at":         datetime.utcnow().isoformat(),
            "source":               "ai",
        }

        # STEP 7B: Store in cache
        self._store_plan(user_id, plan, context)

        return plan

    # ── STEP 7: SMART CACHE ───────────────────────────────────────────────────

    def _build_cache_key(self, user_id: int) -> str:
        """
        Cache key = hash of last activity date + goal id.
        Changes only when new data arrives or goal changes.
        """
        goal = (
            Goal.query.filter_by(user_id=user_id)
            .order_by(Goal.id.desc())
            .first()
        )
        last_activity = (
            Activity.query.filter_by(user_id=user_id)
            .order_by(Activity.date.desc())
            .first()
        )
        key_str = (
            f"v9"  # bumped: use activity id (not date) so re-syncing same activity doesn't bust cache
            f":{user_id}"
            f":{goal.id if goal else 'none'}"
            f":{goal.goal_time if goal else ''}"
            f":{goal.race_date if goal else ''}"
            f":{last_activity.id if last_activity else 'none'}"
        )
        return hashlib.md5(key_str.encode()).hexdigest()[:16]

    def _get_cached_plan(self, user_id: int) -> dict | None:
        """
        Return cached plan if:
        - Exists in DB
        - Generated today (same calendar day)
        - Cache key matches (no new activities or goal changes)
        - It's not Monday morning (force weekly refresh)
        """
        cached = CoachingPlan.query.filter_by(user_id=user_id).first()
        if not cached:
            return None

        today = date.today()

        # Force refresh every Monday if generated before today
        if today.weekday() == 0 and cached.generated_at.date() < today:
            return None

        # Force refresh if generated before today
        if cached.generated_at.date() < today:
            return None

        # Force refresh if cache key changed (new activities or goal change)
        current_key = self._build_cache_key(user_id)
        if cached.cache_key != current_key:
            return None

        try:
            return json.loads(cached.plan_json)
        except Exception:
            return None

    def _store_plan(self, user_id: int, plan: dict, context: dict):
        """Store plan in DB, replace existing."""
        existing = CoachingPlan.query.filter_by(user_id=user_id).first()
        cache_key = self._build_cache_key(user_id)
        plan_json = json.dumps(plan, default=str)
        if existing and existing.context_json:
            try:
                existing_context = json.loads(existing.context_json)
            except Exception:
                existing_context = {}
            freeze_state = existing_context.get("freeze_state")
            if freeze_state is not None:
                context = {**context, "freeze_state": freeze_state}
        context_json = json.dumps(context, default=str)

        feas_score = plan.get("feasibility", {}).get("feasibility_score", 0)
        phase = plan.get("phase", "base")
        weekly_km = plan.get("this_week", {}).get("weekly_target_km", 0)
        long_km = plan.get("this_week", {}).get("long_run", {}).get("km", 0)

        if existing:
            existing.generated_at    = datetime.utcnow()
            existing.plan_json       = plan_json
            existing.context_json    = context_json
            existing.feasibility_score = feas_score
            existing.phase           = phase
            existing.weekly_target_km = weekly_km
            existing.long_run_km     = long_km
            existing.cache_key       = cache_key
        else:
            db.session.add(CoachingPlan(
                user_id=user_id,
                generated_at=datetime.utcnow(),
                plan_json=plan_json,
                context_json=context_json,
                feasibility_score=feas_score,
                phase=phase,
                weekly_target_km=weekly_km,
                long_run_km=long_km,
                cache_key=cache_key,
            ))

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Failed to store coaching plan: {e}")

    # ── BUILD FULL CONTEXT ────────────────────────────────────────────────────

    def _build_full_context(self, user_id: int) -> dict | None:
        goal = (
            Goal.query.filter_by(user_id=user_id)
            .order_by(Goal.id.desc())
            .first()
        )
        if not goal:
            return None

        today = date.today()
        weeks_to_race = (
            round((goal.race_date - today).days / 7, 1)
            if goal.race_date else 0
        )

        profile = RunnerProfile.query.filter_by(user_id=user_id).first()
        profile_data = {}
        if profile:
            profile_data = {
                "training_days_per_week": profile.training_days_per_week or 5,
                "long_run_day":           profile.long_run_day or "sunday",
                "strength_days_per_week": profile.strength_days_per_week or 2,
                "preferred_run_time":     profile.preferred_run_time or "flexible",
                "consistency_level":      profile.consistency_level or "consistent",
                "race_experience":        profile.race_experience or "multiple",
                "injury_status":          profile.injury_status or "healthy",
                "injury_area":            profile.injury_area or "none",
                "goal_priority":          profile.goal_priority or "hit_time",
            }

        dq          = DataQualityReport(user_id)
        compliance  = ComplianceEngine(user_id)
        feasibility = FeasibilityEngine(user_id)

        cutoff = datetime.combine(today - timedelta(weeks=16), datetime.min.time())
        activities = (
            Activity.query.filter(
                Activity.user_id == user_id,
                Activity.date >= cutoff,
            )
            .order_by(Activity.date.asc())
            .all()
        )

        weekly_summaries  = self._build_weekly_summaries(activities, today)
        training_pattern  = self._analyze_training_pattern(activities, profile_data)

        # Resolve long run day: prefer detected pattern over profile preference
        # when there is enough data and the detected day differs
        detected_long = training_pattern.get("detected_long_run_day")
        preferred_long = profile_data.get("long_run_day", "sunday")
        resolved_long = (
            detected_long
            if training_pattern.get("has_enough_pattern_data") and detected_long
            else preferred_long
        )
        training_pattern["resolved_long_run_day"] = resolved_long

        long_runs = sorted(
            [
                {
                    "date": a.date.strftime("%Y-%m-%d"),
                    "distance_km": round(a.distance_km or 0, 1),
                    "pace_min_per_km": round(
                        (a.moving_time / 60) / a.distance_km, 2
                    ) if a.distance_km and a.moving_time else 0,
                }
                for a in activities
                if (a.distance_km or 0) >= 15
            ],
            key=lambda x: x["date"],
            reverse=True,
        )[:8]

        feas_dict = feasibility.to_dict()
        _tsb = feas_dict.get("readiness", {}).get("tsb", 0) or \
               feas_dict.get("readiness", {}).get("current_tsb", 0) or 0
        _atl = feas_dict.get("readiness", {}).get("atl", 0) or 0
        tsb_enforcement = self._tsb_enforcement(float(_tsb), float(_atl))

        _comp_dict      = compliance.to_dict()
        _readiness      = feas_dict.get("readiness", {}) or {}
        _last_actual_km = float(_comp_dict.get("actual_run_km") or 0)
        _curr_weekly    = float(_readiness.get("current_weekly_avg_km") or _last_actual_km or 0)
        _peak_weekly    = float(_readiness.get("required_peak_weekly_km") or 80)
        _curr_long      = float(_readiness.get("current_long_run_km") or 0)
        _req_long       = float(_readiness.get("required_long_run_km") or 32)
        training_targets = {
            "estimated_weekly_target": round(min(_curr_weekly * 1.10, _peak_weekly), 1),
            "estimated_long_run_km":   round(min(_curr_long + 3, _req_long), 1),
        }

        return {
            "runner_profile":   profile_data,
            "tsb_enforcement":  tsb_enforcement,
            "training_targets": training_targets,
            "goal": {
                "race_name":        goal.race_name,
                "race_date":        goal.race_date.strftime("%Y-%m-%d"),
                "goal_time":        goal.goal_time,
                "goal_distance_km": float(goal.race_distance or 42.195),
                "weeks_to_race":    weeks_to_race,
                "today":            today.strftime("%Y-%m-%d"),
                "personal_best":    goal.personal_best,
                "pb_5k":            goal.pb_5k,
                "pb_10k":           goal.pb_10k,
                "pb_hm":            goal.pb_hm,
            },
            "data_quality":    dq.to_dict(),
            "compliance":      compliance.to_dict(),
            "feasibility":     feas_dict,
            "weekly_history":  weekly_summaries,
            "long_run_history": long_runs,
            "training_pattern": training_pattern,
        }

    def _build_weekly_summaries(self, activities: list, today: date) -> list:
        weekly: dict = defaultdict(lambda: {
            "total_km": 0.0, "long_run_km": 0.0, "runs": 0, "week_start": ""
        })
        for a in activities:
            act_date = a.date if isinstance(a.date, date) else a.date.date()
            wk_str = (act_date - timedelta(days=act_date.weekday())).strftime("%Y-%m-%d")
            weekly[wk_str]["total_km"]    += a.distance_km or 0
            weekly[wk_str]["runs"]        += 1
            weekly[wk_str]["week_start"]  = wk_str
            if (a.distance_km or 0) > weekly[wk_str]["long_run_km"]:
                weekly[wk_str]["long_run_km"] = a.distance_km or 0

        result = []
        for i in range(16):
            wk = today - timedelta(weeks=15 - i)
            wk = wk - timedelta(days=wk.weekday())
            wk_str = wk.strftime("%Y-%m-%d")
            entry = weekly.get(wk_str, {
                "total_km": 0.0, "long_run_km": 0.0, "runs": 0, "week_start": wk_str
            })
            entry["week_start"]   = wk_str
            entry["total_km"]     = round(entry["total_km"], 1)
            entry["long_run_km"]  = round(entry["long_run_km"], 1)
            result.append(entry)
        return result

    # ── TSB ENFORCEMENT ──────────────────────────────────────────────────────

    def _tsb_enforcement(self, tsb: float, atl: float) -> dict:
        """
        Convert current TSB (Form) into hard training constraints.

        Zones:
          danger    TSB <= -20   : Cut volume 30%, no quality, mandatory rest alert
          fatigued  TSB in (-20, -10]: Cut volume 15%, no quality
          normal    TSB in (-10, +10]: Standard training
          fresh     TSB in (+10, +20]: Can step up volume 10%
          peak_form TSB > +20    : Alert — possible over-taper or illness
        """
        if tsb <= -20:
            return {
                "zone": "danger",
                "volume_cap": 0.70,
                "long_run_cap": 0.75,
                "quality_allowed": False,
                "mandatory_alert": {
                    "type": "danger",
                    "message": (
                        f"High fatigue detected (Form {tsb:+.0f}). "
                        "Volume capped at 70% of normal. No quality sessions this week."
                    ),
                },
            }
        elif tsb <= -10:
            return {
                "zone": "fatigued",
                "volume_cap": 0.85,
                "long_run_cap": 0.90,
                "quality_allowed": False,
                "mandatory_alert": None,
            }
        elif tsb <= 10:
            return {
                "zone": "normal",
                "volume_cap": 1.00,
                "long_run_cap": 1.00,
                "quality_allowed": True,
                "mandatory_alert": None,
            }
        elif tsb <= 20:
            return {
                "zone": "fresh",
                "volume_cap": 1.10,
                "long_run_cap": 1.05,
                "quality_allowed": True,
                "mandatory_alert": None,
            }
        else:
            return {
                "zone": "peak_form",
                "volume_cap": 1.00,
                "long_run_cap": 1.00,
                "quality_allowed": True,
                "mandatory_alert": {
                    "type": "info",
                    "message": (
                        f"Very high freshness detected (Form {tsb:+.0f}). "
                        "You may be under-training — consider a moderate step-up if feeling good."
                    ),
                },
            }

    # ── PATTERN DETECTION ────────────────────────────────────────────────────

    _RUN_TYPES      = {"run", "virtualrun", "trail run", "trail_run", "treadmill", "track"}
    _STRENGTH_TYPES = {"strength", "weight_training", "strength_training", "crossfit",
                       "yoga", "pilates", "workout", "core", "flexibility"}
    _DAY_NAMES      = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    def _analyze_training_pattern(self, activities: list, profile: dict) -> dict:
        """
        Detect actual training pattern from 8 weeks of activity history.
        Returns day-of-week tendencies for runs, strength, long run, and rest.
        Used to make the AI prompt more accurate about the runner's real schedule.
        """
        from collections import defaultdict

        run_counts      = defaultdict(int)
        strength_counts = defaultdict(int)
        run_distances   = defaultdict(list)  # day → list of km

        for a in activities:
            act_date = a.date if isinstance(a.date, date) else a.date.date()
            typ      = (a.activity_type or "").lower()
            dow      = self._DAY_NAMES[act_date.weekday()]

            if typ in self._RUN_TYPES:
                run_counts[dow] += 1
                run_distances[dow].append(a.distance_km or 0)
            elif typ in self._STRENGTH_TYPES:
                strength_counts[dow] += 1

        # A day is a "typical run day" if it has a run in ≥40% of the 8 weeks
        threshold    = max(2, len(activities) // 10)
        typical_runs = [d for d in self._DAY_NAMES if run_counts[d] >= threshold]

        # Strength day: ≥2 appearances
        typical_strength = [d for d in self._DAY_NAMES if strength_counts[d] >= 2]

        # Long run day: highest average distance across run days
        avg_dist = {
            d: sum(dists) / len(dists)
            for d, dists in run_distances.items()
            if dists
        }
        detected_long_day = (
            max(avg_dist, key=avg_dist.get)
            if avg_dist else profile.get("long_run_day", "sunday")
        )

        all_active = set(typical_runs) | set(typical_strength)
        rest_days  = [d for d in self._DAY_NAMES if d not in all_active]

        has_enough = len(activities) >= 10

        return {
            "typical_run_days":      typical_runs,
            "typical_strength_days": typical_strength,
            "detected_long_run_day": detected_long_day,
            "rest_days":             rest_days,
            "has_enough_pattern_data": has_enough,
            "run_day_counts":        dict(run_counts),
            "strength_day_counts":   dict(strength_counts),
            "avg_run_distance_by_day": {d: round(v, 1) for d, v in avg_dist.items()},
        }

    # ── STEP 5A: AI CALL — WEEKLY PLAN ───────────────────────────────────────

    def _call_claude_weekly(self, context: dict) -> dict:
        """Call A: Generate phase assessment + this week's plan. max_tokens=1500."""
        profile    = context["runner_profile"]
        goal       = context["goal"]
        compliance = context["compliance"]
        feasibility = context["feasibility"]
        tsb_enforcement = context.get("tsb_enforcement", {})

        recent_weeks      = context["weekly_history"][-4:]
        long_run_history  = context["long_run_history"][:5]

        injury_line = (
            f"- Injury area: {profile.get('injury_area')}"
            if profile.get("injury_area") not in ("none", None, "")
            else ""
        )

        pattern      = context.get("training_pattern", {})
        long_run_day = profile.get("long_run_day", "sunday")
        strength_days = profile.get("strength_days_per_week", 2)
        training_days = profile.get("training_days_per_week", 5)
        run_only_days = training_days - strength_days
        active_recovery_days = 7 - training_days

        pattern_confidence = (
            "HIGH — supplements profile, does not override it"
            if pattern.get("has_enough_pattern_data")
            else "LOW — insufficient data, rely on profile only"
        )

        prompt = f"""You are an elite marathon coach. Analyze this runner's data and generate their weekly training plan.

GOAL: {goal['goal_time']} {goal['race_name']} on {goal['race_date']} ({goal['weeks_to_race']} weeks away)

RUNNER PREFERENCES:
- Training days/week: {training_days}
- Long run day: {long_run_day}
- Strength sessions/week: {strength_days}
- Experience: {profile.get('race_experience', 'multiple')}
- Injury status: {profile.get('injury_status', 'healthy')}
{injury_line}
- Goal priority: {profile.get('goal_priority', 'hit_time')}

SCHEDULE CONSTRAINTS FROM RUNNER PROFILE (ground truth — overrides everything else):
- Total training days per week: {training_days}
- Strength sessions per week: {strength_days}
- Long run day: {long_run_day} (FIXED — never change this)
- Run-only days: {run_only_days} days
- Active recovery days: {active_recovery_days} (easy 5km jog/walk — NOT complete rest)

SCHEDULE BUILDING RULE — follow these steps in order:
Step 1: Assign long run to {long_run_day}
Step 2: Assign {strength_days} strength days (prefer Wed and Fri based on pattern)
Step 3: Fill remaining {run_only_days - 1} training days with runs (easy/tempo)
Step 4: Remaining {active_recovery_days} days = active_recovery (5km easy jog — not rest)
Step 5: NEVER leave any day as complete rest — every day has a purpose

ACTUAL TRAINING PATTERN (detected from Strava — supplements profile, does not override):
- Typical run days: {pattern.get('typical_run_days', [])}
- Typical strength days: {pattern.get('typical_strength_days', [])}
- Detected long run day: {pattern.get('resolved_long_run_day', long_run_day)}
- Pattern confidence: {pattern_confidence}
- Run frequency per day: {pattern.get('run_day_counts', {})}

CURRENT FITNESS:
- Feasibility score: {feasibility.get('feasibility_score', 0)}/100
- Assessment: {feasibility.get('assessment_label', 'unknown')}
- Current VDOT: {feasibility.get('vdot', {}).get('current', 0)}
- Required VDOT: {feasibility.get('vdot', {}).get('required', 0)}
- Already aerobically capable: {feasibility.get('vdot', {}).get('already_capable', False)}
- Current weekly avg: {feasibility.get('readiness', {}).get('current_weekly_avg_km', 0)}km
- Required peak weekly: {feasibility.get('readiness', {}).get('required_peak_weekly_km', 0)}km
- Current long run: {feasibility.get('readiness', {}).get('current_long_run_km', 0)}km
- Required long run: {feasibility.get('readiness', {}).get('required_long_run_km', 0)}km
- Consistency score: {feasibility.get('consistency_score', 0)}/100

LAST WEEK:
- Planned: {compliance.get('planned_run_km', 0)}km
- Actual: {compliance.get('actual_run_km', 0)}km
- Compliance: {compliance.get('volume_compliance_pct', 0)}%
- Miss reason: {compliance.get('miss_reason', {}).get('code', 'unknown')}
- Trend: {compliance.get('trend', {}).get('direction', 'unknown')}
- Consecutive good weeks: {compliance.get('trend', {}).get('consecutive_good_weeks', 0)}

RECENT 4 WEEKS:
{json.dumps(recent_weeks, indent=2)}

RECENT LONG RUNS:
{json.dumps(long_run_history, indent=2)}

FACTUAL CONSTRAINTS (use these exact numbers — do not invent or approximate):
- This week's target volume: {context.get('training_targets', {}).get('estimated_weekly_target', 0)}km
- This week's long run target: {context.get('training_targets', {}).get('estimated_long_run_km', 0)}km
- When you mention weekly km or long run distance in your response, use these numbers exactly.

LOAD-BASED HARD CONSTRAINTS (TSB-driven — override coaching rules if they conflict):
- Current Form (TSB): {feasibility.get('readiness', {}).get('tsb', 0) or 0:+.1f}
- Fatigue zone: {tsb_enforcement.get('zone', 'normal')}
- Volume cap this week: {tsb_enforcement.get('volume_cap', 1.00) * 100:.0f}% of normal
- Long run cap: {tsb_enforcement.get('long_run_cap', 1.00) * 100:.0f}% of planned long run
- Quality sessions allowed: {tsb_enforcement.get('quality_allowed', True)}

{"⚠️ HIGH FATIGUE: Replace any tempo/intervals/marathon_pace sessions with easy or active_recovery." if not tsb_enforcement.get('quality_allowed', True) else ""}

COACHING RULES (non-negotiable):
1. Never increase weekly volume more than 10% from last week actual
2. Never increase long run more than 3km from recent long run
3. Apply 3:1 pattern: every 4th week is recovery (80% volume)
4. Long run on runner's preferred day ({profile.get('long_run_day', 'sunday')})
5. Strength sessions on non-run days only
6. If injury_status is 'ongoing' — reduce intensity, flag it
7. If compliance last week < 70% — adjust this week DOWN not up
8. If consecutive_good_weeks >= 3 — can step up 10%
9. Base weekly volume on recent ACTUAL km, not planned

HARD CONSTRAINTS — never violate these:
1. Training days = {profile.get('training_days_per_week', 5)}, so rest days = {7 - profile.get('training_days_per_week', 5)} maximum
2. If typical_run_days has >= 4 days, never assign 3+ consecutive rest days
3. Strength counts as a training day — never mark a strength day as rest
4. The daily_plan must have exactly {profile.get('training_days_per_week', 5)} non-rest days
5. Long run day ({pattern.get('resolved_long_run_day', profile.get('long_run_day', 'sunday'))}) must ALWAYS have a long run — never rest on long run day
6. Today is {date.today().strftime('%A').lower()} — if runner typically runs today, assign a run

BUILD DAILY PLAN USING THIS EXACT LOGIC:
Typical run days: {pattern.get('typical_run_days', [])}
Typical strength days: {pattern.get('typical_strength_days', [])}
Rest days: {pattern.get('rest_days', [])}

For each day of the week:
- If day is in typical_run_days → assign run (long/tempo/easy based on week structure)
- If day is in typical_strength_days → assign strength
- If day is in rest_days AND we still need more rest days → assign rest
- NEVER assign rest to a day in typical_run_days
- NEVER assign a run to a day in rest_days

Respond ONLY with valid JSON, no markdown:
{{
  "phase": "rebuild|base|build|peak|taper",
  "phase_label": "human readable label",
  "phase_reasoning": "2 sentences max — why this phase",
  "week_theme": "e.g. Rebuild Base — Easy Effort Only",
  "is_recovery_week": true,

  "this_week": {{
    "weekly_target_km": 0.0,
    "daily_plan": {{
      "monday":    {{"type": "easy|tempo|long|active_recovery|strength|recovery|intervals|marathon_pace", "km": 0.0, "pace_guidance": "", "notes": ""}},
      "tuesday":   {{"type": "", "km": 0.0, "pace_guidance": "", "notes": ""}},
      "wednesday": {{"type": "strength", "km": 0, "pace_guidance": "", "notes": "Gym — strength and conditioning"}},
      "thursday":  {{"type": "", "km": 0.0, "pace_guidance": "", "notes": ""}},
      "friday":    {{"type": "strength", "km": 0, "pace_guidance": "", "notes": "Gym — strength and conditioning"}},
      "saturday":  {{"type": "", "km": 0.0, "pace_guidance": "", "notes": ""}},
      "sunday":    {{"type": "long", "km": 0.0, "pace_guidance": "", "notes": ""}}
    }},
    "long_run": {{
      "km": 0.0,
      "pace_guidance": "e.g. 6:30-7:00/km conversational",
      "purpose": "e.g. Rebuild aerobic base after training gap"
    }},
    "quality_session": {{
      "day": "tuesday|thursday",
      "type": "tempo|intervals|marathon_pace|none",
      "km": 0.0,
      "pace_guidance": "",
      "description": "full workout description"
    }},
    "focus_point": "single most important thing this week",
    "compliance_response": "acknowledge last week honestly, adapt"
  }},

  "coaching_message": "3-4 sentences. Personal, specific numbers. Honest.",
  "alerts": [
    {{"type": "warning|info|danger", "message": ""}}
  ]
}}"""

        return self._call_api(prompt, max_tokens=1500)

    # ── STEP 5B: AI CALL — LONG RUN PROGRESSION ──────────────────────────────

    def _call_claude_progression(self, context: dict, weekly_plan: dict) -> list:
        """Call B: Generate full long run progression schedule from today to race day."""
        goal          = context["goal"]
        feasibility   = context["feasibility"]
        recent_long   = feasibility.get("readiness", {}).get("current_long_run_km", 18)
        required_long = feasibility.get("readiness", {}).get("required_long_run_km", 32)
        weeks_to_race = goal["weeks_to_race"]
        phase         = weekly_plan.get("phase", "base")
        long_run_history = context["long_run_history"]
        this_week_long = (
            weekly_plan.get("this_week", {}).get("long_run", {}).get("km", recent_long)
        )

        prompt = f"""You are an elite marathon coach.
Generate a complete long run progression schedule from today to race day.

RACE: {goal['race_name']} on {goal['race_date']}
GOAL TIME: {goal['goal_time']}
WEEKS TO RACE: {weeks_to_race}
CURRENT PHASE: {phase}
RECENT LONG RUN: {recent_long}km
REQUIRED PEAK LONG RUN: {required_long}km
THIS WEEK'S LONG RUN: {this_week_long}km

LONG RUN HISTORY (recent):
{json.dumps(long_run_history, indent=2)}

RULES:
1. Start from THIS WEEK's planned long run
2. Increase max 2km every 2 weeks
3. Every 4th week: recovery (reduce 15-20%)
4. Peak runs: 2-4 runs at maximum distance
5. Maximum = min(38km, goal_distance_km * 0.90)
6. Taper: start 3 weeks before race
   Week -3: 75% of peak
   Week -2: 55% of peak
   Week -1: 30% of peak
7. Race week: 8km easy shakeout
8. Dates must be actual Sundays (or runner's preferred long run day)

Respond ONLY with valid JSON array, no markdown:
[
  {{
    "week_number": 1,
    "week_date": "YYYY-MM-DD",
    "target_km": 0.0,
    "phase": "rebuild|base|build|peak|taper",
    "is_recovery_week": false,
    "is_peak_run": false,
    "label": "e.g. Rebuild | First 30km+ | Peak Run | Taper"
  }}
]
Generate for ALL weeks until race day inclusive."""

        result = self._call_api(prompt, max_tokens=4096)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            for key in ("progression", "long_runs", "schedule", "weeks"):
                if key in result and isinstance(result[key], list):
                    return result[key]
        return []

    # ── CORE API CALL ─────────────────────────────────────────────────────────

    def _call_api(self, prompt: str, max_tokens: int = 1500) -> dict | list:
        """
        Call Claude API with robust error handling.
        Tries JSON extraction multiple ways before failing.
        """
        api_key = current_app.config.get("ANTHROPIC_API_KEY")
        model   = current_app.config.get("ANTHROPIC_MODEL", "claude-sonnet-4-5")

        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not configured")

        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":          api_key,
                "anthropic-version":  "2023-06-01",
                "content-type":       "application/json",
            },
            json={
                "model":      model,
                "max_tokens": max_tokens,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=(5, 20),  # (connect, read) — 2 calls max ~50s, within gunicorn 120s
        )

        if response.status_code != 200:
            raise ValueError(
                f"API error {response.status_code}: {response.text[:200]}"
            )

        data = response.json()
        text = data["content"][0]["text"].strip()

        # Robust JSON extraction — try 3 strategies
        # Try 1: direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try 2: strip markdown fences
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.MULTILINE).strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Try 3: find first JSON object or array in text
        match = re.search(r"(\{.*\}|\[.*\])", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Could not parse JSON from API response: {text[:300]}")

    # ── STEP 6: VALIDATION LAYER ──────────────────────────────────────────────

    def _validate_weekly(self, plan: dict, context: dict) -> dict:
        """
        Validate and correct AI weekly plan.
        Enforces 10% rule, long run cap, phase consistency,
        and runner preference compliance.
        """
        if not plan or "this_week" not in plan:
            return self._fallback_weekly(context)

        this_week       = plan.get("this_week", {})
        compliance      = context["compliance"]
        feasibility     = context["feasibility"]
        profile         = context["runner_profile"]
        tsb_enforcement = context.get("tsb_enforcement", {})

        recent_actual = compliance.get("actual_run_km", 0)
        trend         = compliance.get("trend", {})
        recent_avg    = trend.get("avg_last_2_weeks", recent_actual)
        base_km       = max(recent_avg, recent_actual, 20)

        # Enforce 10% rule on weekly target
        ai_weekly    = float(this_week.get("weekly_target_km", base_km))
        max_allowed  = base_km * 1.10
        vol_adjustment = (
            compliance.get("response", {}).get("volume_adjustment", 1.0)
        )
        adjusted_max = max_allowed * vol_adjustment

        # Apply TSB volume cap on top of 10% rule
        tsb_volume_cap = tsb_enforcement.get("volume_cap", 1.00)
        tsb_adjusted_max = base_km * tsb_volume_cap
        adjusted_max = min(adjusted_max, tsb_adjusted_max)

        weekly_enforced = ai_weekly > adjusted_max
        if weekly_enforced:
            this_week["weekly_target_km"] = round(adjusted_max, 1)
            plan["this_week"] = this_week

        # Enforce long run 3km max increase
        current_long = (
            feasibility.get("readiness", {}).get("current_long_run_km", 18)
        )
        ai_long   = float(this_week.get("long_run", {}).get("km", current_long))
        max_long  = current_long + 3.0

        # Apply TSB long run cap on top of 3km rule
        tsb_long_cap = tsb_enforcement.get("long_run_cap", 1.00)
        max_long = min(max_long, current_long * tsb_long_cap + 3.0 * tsb_long_cap)

        long_enforced = ai_long > max_long
        if long_enforced:
            if "long_run" in this_week:
                this_week["long_run"]["km"] = round(max_long, 1)
                daily = this_week.get("daily_plan", {})
                long_run_day = profile.get("long_run_day", "sunday")
                if long_run_day in daily:
                    daily[long_run_day]["km"] = round(max_long, 1)

        # Replace quality sessions if TSB says no quality allowed
        quality_allowed = tsb_enforcement.get("quality_allowed", True)
        if not quality_allowed:
            quality_types = {"tempo", "intervals", "marathon_pace"}
            daily = this_week.get("daily_plan", {})
            quality_replaced = False
            for day, session in daily.items():
                if session.get("type") in quality_types:
                    session["type"] = "easy"
                    session["notes"] = (
                        "Downgraded from quality session — high fatigue detected. Run easy."
                    )
                    quality_replaced = True
            # Also replace this_week.quality_session
            qs = this_week.get("quality_session", {})
            if qs.get("type") in quality_types:
                qs["type"] = "none"
                qs["description"] = "Quality session removed — recovery required this week."
                this_week["quality_session"] = qs

        # Ensure long run is on preferred day
        long_run_day = profile.get("long_run_day", "sunday")
        daily = this_week.get("daily_plan", {})
        if daily and long_run_day in daily:
            daily[long_run_day]["type"] = "long"

        # Ensure strength days have km=0
        for session in daily.values():
            if session.get("type") == "strength":
                session["km"] = 0

        # Prepend mandatory TSB alert if present
        mandatory_alert = tsb_enforcement.get("mandatory_alert")
        if mandatory_alert:
            existing_alerts = plan.get("alerts", [])
            plan["alerts"] = [mandatory_alert] + existing_alerts

        # Round all daily km values to nearest 0.5 for clean display
        daily = this_week.get("daily_plan", {})
        for session in daily.values():
            if float(session.get("km", 0)) > 0:
                session["km"] = round(float(session["km"]) * 2) / 2

        # Round long run km to nearest 0.5
        if "long_run" in this_week and this_week["long_run"].get("km", 0) > 0:
            this_week["long_run"]["km"] = round(float(this_week["long_run"]["km"]) * 2) / 2
            # Keep long run day in sync
            long_run_day = profile.get("long_run_day", "sunday")
            if long_run_day in daily and daily[long_run_day].get("type") == "long":
                daily[long_run_day]["km"] = this_week["long_run"]["km"]

        plan["validation"] = {
            "weekly_target_enforced":    weekly_enforced,
            "long_run_enforced":         long_enforced,
            "quality_replaced":          not quality_allowed,
            "tsb_zone":                  tsb_enforcement.get("zone", "normal"),
            "tsb_volume_cap":            tsb_volume_cap,
            "base_km_used":              round(base_km, 1),
            "volume_adjustment_applied": vol_adjustment,
        }

        return plan

    def _validate_progression(self, progression: list, context: dict) -> list:
        """
        Validate long run progression.
        Enforces 2km max increase, 3:1 pattern, correct taper, valid dates.
        """
        if not progression:
            return self._fallback_progression(context)

        goal       = context["goal"]
        feasibility = context["feasibility"]
        race_date  = datetime.strptime(goal["race_date"], "%Y-%m-%d").date()
        max_long   = min(38.0, float(goal.get("goal_distance_km", 42.195)) * 0.90)

        validated = []
        prev_km   = None

        for week in progression:
            km = float(week.get("target_km", 18))
            km = min(km, max_long)

            if prev_km and not week.get("is_recovery_week"):
                km = min(km, prev_km + 2.0)

            # Enforce taper based on proximity to race
            week_date_str = week.get("week_date", "")
            try:
                week_date = datetime.strptime(week_date_str, "%Y-%m-%d").date()
                weeks_before_race = (race_date - week_date).days / 7

                if weeks_before_race <= 1:
                    km = min(km, max_long * 0.30)
                    week["phase"] = "taper"
                    week["label"] = "Race Week — Easy Shakeout"
                elif weeks_before_race <= 2:
                    km = min(km, max_long * 0.55)
                    week["phase"] = "taper"
                    week["label"] = "Taper Week 2"
                elif weeks_before_race <= 3:
                    km = min(km, max_long * 0.75)
                    week["phase"] = "taper"
                    week["label"] = "Taper Begins"
            except ValueError:
                pass

            week["target_km"] = round(km, 1)
            if not week.get("is_recovery_week"):
                prev_km = km

            validated.append(week)

        return validated

    # ── FALLBACK PLANS ────────────────────────────────────────────────────────

    def _fallback_weekly(self, context: dict) -> dict:
        """Rule-based fallback when AI call fails. Uses compliance engine output."""
        compliance  = context["compliance"]
        feasibility = context["feasibility"]
        profile     = context.get("runner_profile", {})

        recent_actual = compliance.get("actual_run_km", 30)
        vol_adj       = compliance.get("response", {}).get("volume_adjustment", 1.0)
        weekly_target = round(max(20, min(80, recent_actual * vol_adj * 1.05)), 1)

        current_long  = feasibility.get("readiness", {}).get("current_long_run_km", 18)
        long_run_km   = min(current_long + 2, 32)

        training_days = profile.get("training_days_per_week", 5)
        long_run_day  = profile.get("long_run_day", "sunday")
        easy_km       = round((weekly_target - long_run_km) / max(1, training_days - 1), 1)

        all_days   = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        daily_plan = {}
        run_days   = 0

        for day in all_days:
            if day == long_run_day:
                daily_plan[day] = {
                    "type": "long", "km": long_run_km,
                    "pace_guidance": "Easy conversational pace", "notes": "Long run — time on feet",
                }
                run_days += 1
            elif day in ("wednesday", "friday"):
                daily_plan[day] = {
                    "type": "strength", "km": 0,
                    "pace_guidance": "", "notes": "Gym — strength and conditioning",
                }
            elif run_days < training_days - 1:
                daily_plan[day] = {
                    "type": "easy", "km": easy_km,
                    "pace_guidance": "Easy effort", "notes": "Easy run",
                }
                run_days += 1
            else:
                daily_plan[day] = {
                    "type": "rest", "km": 0, "pace_guidance": "", "notes": "Rest day",
                }

        return {
            "phase":          "base",
            "phase_label":    "Base Building",
            "phase_reasoning": "Building aerobic base consistently.",
            "week_theme":     "Easy Base Building",
            "is_recovery_week": False,
            "this_week": {
                "weekly_target_km": weekly_target,
                "daily_plan":       daily_plan,
                "long_run": {
                    "km":           long_run_km,
                    "pace_guidance": "Easy conversational pace",
                    "purpose":      "Build aerobic base",
                },
                "quality_session": {
                    "day": "tuesday", "type": "none", "km": 0,
                    "pace_guidance": "", "description": "Easy week — no quality session",
                },
                "focus_point":        "Run easy, stay consistent.",
                "compliance_response": "Keep building week by week.",
            },
            "coaching_message": (
                "Focus on consistency this week. "
                "Easy runs build the aerobic base that makes race day possible."
            ),
            "alerts": [],
            "source": "fallback",
        }

    def _fallback_progression(self, context: dict) -> list:
        """Simple rule-based long run progression."""
        goal        = context["goal"]
        feasibility = context["feasibility"]

        current_long  = feasibility.get("readiness", {}).get("current_long_run_km", 18)
        weeks_to_race = float(goal.get("weeks_to_race", 20))
        race_date     = datetime.strptime(goal["race_date"], "%Y-%m-%d").date()
        max_long      = min(32.0, float(goal.get("goal_distance_km", 42.195)) * 0.80)

        today = date.today()
        days_to_sunday = (6 - today.weekday()) % 7 or 7
        next_sunday = today + timedelta(days=days_to_sunday)

        progression = []
        km = current_long

        for week_num in range(1, int(weeks_to_race) + 1):
            week_date = next_sunday + timedelta(weeks=week_num - 1)
            weeks_before_race = (race_date - week_date).days / 7
            is_recovery = week_num % 4 == 0

            if weeks_before_race <= 1:
                km_this = round(max_long * 0.25, 1)
                phase, label = "taper", "Race Week"
            elif weeks_before_race <= 2:
                km_this = round(max_long * 0.50, 1)
                phase, label = "taper", "Taper Week 2"
            elif weeks_before_race <= 3:
                km_this = round(max_long * 0.70, 1)
                phase, label = "taper", "Taper Begins"
            elif is_recovery:
                km_this = round(km * 0.80, 1)
                phase, label = "base", "Recovery Week"
            else:
                km = min(km + 2.0, max_long)
                km_this = round(km, 1)
                phase   = "base"
                label   = "Peak Run" if km >= max_long else "Long Run"

            progression.append({
                "week_number":     week_num,
                "week_date":       week_date.strftime("%Y-%m-%d"),
                "target_km":       km_this,
                "phase":           phase,
                "is_recovery_week": is_recovery,
                "is_peak_run":     km_this >= max_long,
                "label":           label,
            })

        return progression

    def _fallback_plan(self, reason: str, user_id: int) -> dict:
        return {
            "phase":       "base",
            "phase_label": "Getting Started",
            "week_theme":  "Set your goal to begin",
            "this_week": {
                "weekly_target_km": 30,
                "daily_plan":       {},
                "long_run":   {"km": 12, "pace_guidance": "Easy pace", "purpose": "Base building"},
                "focus_point": "Connect Strava and set a race goal to unlock your coaching plan.",
                "compliance_response": "",
                "quality_session": {"type": "none", "km": 0},
            },
            "coaching_message": (
                "Set a race goal and sync your Strava "
                "to unlock your personalized coaching plan."
            ),
            "alerts": [{"type": "info", "message": reason}],
            "long_run_progression": [],
            "feasibility": {},
            "compliance":  {},
            "source": "fallback",
        }
