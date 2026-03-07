# ============================================================
# HEALTH ENGINE
# Handles RHR baseline comparison and recovery override logic
# ============================================================

from database import get_last_7_day_hr_avg, log_health_status


def process_health_input(athlete_id, resting_hr, illness_flag):

    baseline = get_last_7_day_hr_avg(athlete_id)

    if baseline is None:
        hr_delta = 0
    else:
        hr_delta = resting_hr - baseline

    override_active = 0

    # ---- Health Override Logic ----
    if hr_delta >= 8:
        override_active = 1

    if illness_flag == 1:
        override_active = 1

    log_health_status(athlete_id, resting_hr, hr_delta, illness_flag, override_active)

    return hr_delta, override_active