import json

import requests
from flask import current_app


def _heuristic_recommendation(goal, intel, milestone, recent_metrics):
    if not intel:
        return "No enough training data yet. Sync Strava and complete 3+ runs to unlock AI recommendations."

    probability = intel.get("probability", 0)
    ctl = intel.get("current_ctl", 0)
    tsb = 0
    if recent_metrics:
        tsb = float(recent_metrics[0]["tsb"] or 0)

    if tsb < -15:
        recovery_note = "You are carrying fatigue, so prioritize an easy day."
    elif tsb > 10:
        recovery_note = "You are fresh enough for quality work."
    else:
        recovery_note = "You are in a neutral readiness zone."

    if probability < 45:
        core = "Run 60-75 minutes easy with 6 strides, then add one aerobic long run this week."
    elif probability < 70:
        core = "Schedule a tempo workout: 3 x 10 min at controlled threshold with easy recoveries."
    else:
        core = "Use race-specific work: 14-18 km including marathon-pace segments."

    return (
        f"AI Coach (fallback): {core} "
        f"Current CTL {ctl}, milestone {milestone['current_long']} km, target race {goal['event_name']}. "
        f"{recovery_note}"
    )


def generate_ai_recommendation(goal, intel, milestone, recent_metrics):
    api_key = current_app.config.get("OPENAI_API_KEY")
    if not api_key:
        return _heuristic_recommendation(goal, intel, milestone, recent_metrics)

    prompt = {
        "goal": goal,
        "projection": intel,
        "milestone": milestone,
        "recent_metrics": [
            {
                "timestamp": row["timestamp"],
                "distance_km": row["distance_km"],
                "stress": row["stress"],
                "ctl": row["ctl"],
                "tsb": row["tsb"],
                "readiness": row["readiness"],
            }
            for row in recent_metrics[:14]
        ],
    }

    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": current_app.config.get("OPENAI_MODEL", "gpt-4.1-mini"),
            "input": [
                {
                    "role": "system",
                    "content": "You are an elite endurance coach. Give one actionable session recommendation and one recovery note.",
                },
                {
                    "role": "user",
                    "content": json.dumps(prompt),
                },
            ],
            "max_output_tokens": 180,
        },
        timeout=20,
    )

    if response.status_code >= 400:
        return _heuristic_recommendation(goal, intel, milestone, recent_metrics)

    payload = response.json()
    text = payload.get("output_text")
    if text:
        return text.strip()

    return _heuristic_recommendation(goal, intel, milestone, recent_metrics)
