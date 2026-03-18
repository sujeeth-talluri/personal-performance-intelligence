import math
from datetime import timedelta

# Activity type categorization
DISTANCE_ACTIVITY_TYPES = {"run", "trailrun", "walk", "hike", "ride", "swim"}
RUN_ACTIVITY_TYPES = {"run", "trailrun"}

# Cross-training intensity factors (used as IF for non-run TSS)
STRESS_TYPE_FACTOR = {
    "run": 1.0,
    "trailrun": 1.0,
    "walk": 0.55,
    "hike": 0.60,
    "ride": 0.85,
    "swim": 0.80,
    "strength": 0.70,
    "yoga": 0.35,
}

# Zone-based intensity factors for runs (IF values used in TSS formula)
ZONE_IF = {
    "zone1": 0.65,  # Recovery:  > threshold + 90 sec/km
    "zone2": 0.75,  # Easy:      threshold + 45–90 sec/km
    "zone3": 0.85,  # Aerobic:   threshold + 15–45 sec/km
    "zone4": 1.00,  # Threshold: threshold ± 15 sec/km
    "zone5": 1.10,  # VO2max:    < threshold - 15 sec/km
}

# Legacy alias kept for backwards-compatible imports in analytics_service
RUN_INTENSITY_FACTOR = ZONE_IF

# Exponential decay constants (pre-computed for performance)
_ATL_DECAY = math.exp(-1.0 / 7.0)   # ≈ 0.8668
_CTL_DECAY = math.exp(-1.0 / 42.0)  # ≈ 0.9768
_ATL_GAIN = 1.0 - _ATL_DECAY        # ≈ 0.1332
_CTL_GAIN = 1.0 - _CTL_DECAY        # ≈ 0.0232


def _threshold_pace(marathon_pace_sec_per_km: float) -> float:
    """Estimate lactate threshold pace from marathon goal pace.

    LT pace ≈ half-marathon race pace + 15 sec/km.
    HM pace ≈ marathon pace × 0.95 (Riegel approximation for the half).
    """
    hm_pace = marathon_pace_sec_per_km * 0.95
    return hm_pace + 15.0


def _pace_to_zone(pace_sec_per_km: float, threshold: float) -> str:
    """Map a run pace to a training zone relative to threshold pace.

    Zone 5 (VO2max)   : pace <  threshold - 15
    Zone 4 (Threshold): threshold - 15 <= pace <= threshold + 15
    Zone 3 (Aerobic)  : threshold + 15 <  pace <= threshold + 45
    Zone 2 (Easy)     : threshold + 45 <  pace <= threshold + 90
    Zone 1 (Recovery) : pace >  threshold + 90
    """
    delta = pace_sec_per_km - threshold  # positive = slower than threshold
    if delta > 90:
        return "zone1"
    if delta > 45:
        return "zone2"
    if delta > 15:
        return "zone3"
    if delta >= -15:
        return "zone4"
    return "zone5"


def classify_run_intensity(run, marathon_pace_sec_per_km):
    """Classify run intensity using threshold-relative pace zones.

    Returns labels compatible with downstream analytics:
        recovery, easy, easy_long, aerobic, steady_long,
        marathon_specific, marathon_specific_long, tempo, speed, unknown
    """
    run_pace = run.get("pace_sec_per_km")
    distance_km = float(run.get("distance_km") or 0.0)
    if not run_pace or run_pace <= 0 or marathon_pace_sec_per_km <= 0:
        return "unknown"

    threshold = _threshold_pace(marathon_pace_sec_per_km)
    zone = _pace_to_zone(run_pace, threshold)
    long = distance_km >= 18

    # Map zones to legacy label names expected by analytics/prediction engines
    if zone == "zone5":
        return "speed"
    if zone == "zone4":
        if long:
            return "marathon_specific_long"
        if distance_km >= 12:
            return "marathon_specific"
        return "tempo"
    if zone == "zone3":
        return "steady_long" if long else "aerobic"
    if zone == "zone2":
        return "easy_long" if long else "easy"
    # zone1
    if long:
        return "easy_long"
    if distance_km >= 8:
        return "easy"
    return "recovery"


def running_stress_score(activity, marathon_pace_sec_per_km):
    """Compute Training Stress Score (TSS) using the standard PMC formula.

    For runs:
        IF   = zone intensity factor derived from pace vs threshold pace
        TSS  = (duration_sec × IF²) / 3600 × 100 × elevation_factor

    For cross-training:
        IF   = sport-specific type factor
        TSS  = (duration_sec × IF²) / 3600 × 100 × elevation_factor
    """
    activity_type = activity["type"]
    duration_sec = float(activity.get("moving_time_sec") or 0.0)
    elevation_gain = float(activity.get("elevation_gain") or 0.0)
    elevation_factor = 1.0 + min(0.12, elevation_gain / 5000.0)

    if duration_sec <= 0:
        return 0.0

    if activity_type in RUN_ACTIVITY_TYPES:
        distance_km = float(activity.get("distance_km") or 0.0)
        if distance_km < 3.0:
            return 0.0

        threshold = _threshold_pace(marathon_pace_sec_per_km)
        run_pace = activity.get("pace_sec_per_km")
        if run_pace and run_pace > 0:
            zone = _pace_to_zone(run_pace, threshold)
        else:
            zone = "zone3"  # default to aerobic when pace unavailable

        intensity_factor = ZONE_IF[zone]
        tss = (duration_sec * intensity_factor ** 2) / 3600.0 * 100.0
        return round(tss * elevation_factor, 1)

    type_if = STRESS_TYPE_FACTOR.get(activity_type, 0.5)
    tss = (duration_sec * type_if ** 2) / 3600.0 * 100.0
    return round(tss * elevation_factor, 1)


def load_model(load_activities, today, marathon_pace_sec_per_km, days=14):
    """Compute ATL, CTL, and TSB using proper exponential decay (PMC model).

    ATL_t = ATL_{t-1} × exp(-1/7)  + TSS_t × (1 - exp(-1/7))
    CTL_t = CTL_{t-1} × exp(-1/42) + TSS_t × (1 - exp(-1/42))
    TSB_t = CTL_t - ATL_t

    Returns a dict with ctl_today, atl_today, tsb_today, fatigue_ratio,
    and 14-day ctl_series / atl_series for charting.
    """
    if load_activities:
        earliest = min(a["date"] for a in load_activities)
        start = min(earliest, today - timedelta(days=120))
    else:
        start = today - timedelta(days=120)

    daily_tss: dict = {}
    for activity in load_activities:
        stress = running_stress_score(activity, marathon_pace_sec_per_km)
        daily_tss[activity["date"]] = daily_tss.get(activity["date"], 0.0) + stress

    ctl = 0.0
    atl = 0.0
    ctl_timeline: dict = {}
    atl_timeline: dict = {}

    day = start
    while day <= today:
        tss = daily_tss.get(day, 0.0)
        ctl = ctl * _CTL_DECAY + tss * _CTL_GAIN
        atl = atl * _ATL_DECAY + tss * _ATL_GAIN
        ctl_timeline[day] = round(ctl, 1)
        atl_timeline[day] = round(atl, 1)
        day += timedelta(days=1)

    series_start = today - timedelta(days=days - 1)
    ctl_out = []
    atl_out = []
    for i in range(days):
        day = series_start + timedelta(days=i)
        label = f"{day.strftime('%b')} {day.day}"
        ctl_out.append({"date": day.isoformat(), "label": label, "value": ctl_timeline.get(day, 0.0)})
        atl_out.append({"date": day.isoformat(), "label": label, "value": atl_timeline.get(day, 0.0)})

    ctl_today = round(ctl_timeline.get(today, 0.0), 1)
    atl_today = round(atl_timeline.get(today, 0.0), 1)
    tsb_today = round(ctl_today - atl_today, 1)
    fatigue_ratio = round(atl_today / max(1.0, ctl_today), 2)
    return {
        "ctl_today": ctl_today,
        "atl_today": atl_today,
        "tsb_today": tsb_today,
        "fatigue_ratio": fatigue_ratio,
        "ctl_series": ctl_out,
        "atl_series": atl_out,
    }
