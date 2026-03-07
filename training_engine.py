# ============================================================
# DURABILITY-AWARE TRAINING ENGINE
# ============================================================

from database import fetch_all_metrics
from datetime import datetime, date, timedelta


def generate_week_plan(athlete_id, current_ctl, weeks_left):

    rows = fetch_all_metrics(athlete_id)

    # ---- Calculate Durability ----
    eight_weeks_ago = date.today() - timedelta(weeks=8)

    max_long = 0

    for row in rows:
        _, _, timestamp, distance_km, *_ = row
        run_date = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").date()

        if run_date >= eight_weeks_ago:
            if distance_km >= 40:
                continue
            max_long = max(max_long, distance_km)

    # ---- Base Weekly Volume Target ----
    if current_ctl < 45:
        target_km = 60
    elif current_ctl < 55:
        target_km = 70
    else:
        target_km = 80

    # ---- Durability Phase Logic ----
    if max_long < 24:
        phase = "STRUCTURAL BUILD"
        long_run_km = min(max_long + 2, 28)
        key_session = "Aerobic Endurance Focus (No aggressive tempo)"

    elif max_long < 30:
        phase = "DEPTH BUILD"
        long_run_km = min(max_long + 2, 32)
        key_session = "Light Tempo + Endurance Blend"

    else:
        phase = "PERFORMANCE BUILD"
        long_run_km = max_long
        key_session = "Tempo Intervals (Marathon pace focus)"

    return {
        "phase": phase,
        "target_km": target_km,
        "long_run_km": round(long_run_km, 1),
        "key_session": key_session
    }