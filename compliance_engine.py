from datetime import date
from database import (
    save_prescribed_session,
    fetch_today_prescription,
    update_compliance,
    weekly_compliance as db_weekly_compliance,
    fetch_latest_metric
)

def generate_and_store_prescription(plan):
    today = date.today().isoformat()

    # Example logic based on plan
    if "Tempo" in plan["key_session"]:
        session_type = "Tempo"
        min_km = 8
        max_km = 14
        intensity = "Moderate-High"
    else:
        session_type = "Aerobic"
        min_km = 8
        max_km = 16
        intensity = "Easy"

    save_prescribed_session(athlete_id, today, session_type, min_km, max_km, intensity)


def evaluate_compliance():
    prescription = fetch_today_prescription(athlete_id)
    last_activity = fetch_latest_metric()

    if not prescription or not last_activity:
        return

    session_date, session_type, min_km, max_km, intensity, completed, _ = prescription
    distance_km = last_activity[2]

    tolerance_min = min_km * 0.8
    tolerance_max = max_km * 1.2

    if tolerance_min <= distance_km <= tolerance_max:
        score = 100
    elif distance_km >= min_km * 0.5:
        score = 75
    else:
        score = 50

    update_compliance(session_date, score)


def weekly_compliance(athlete_id):
    return db_weekly_compliance(athlete_id)