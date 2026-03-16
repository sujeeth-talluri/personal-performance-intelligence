from datetime import timedelta

THRESHOLD_HR = 168
DISTANCE_ACTIVITY_TYPES = {"run", "trailrun", "walk", "hike", "ride", "swim"}
RUN_ACTIVITY_TYPES = {"run", "trailrun"}
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
RUN_INTENSITY_FACTOR = {
    "recovery": 0.85,
    "easy": 1.0,
    "aerobic": 1.1,
    "steady": 1.15,
    "steady_long": 1.2,
    "easy_long": 1.1,
    "marathon_specific": 1.2,
    "marathon_specific_long": 1.25,
    "tempo": 1.3,
    "speed": 1.5,
    "unknown": 1.0,
}


def classify_run_intensity(run, marathon_pace_sec_per_km):
    run_pace = run.get("pace_sec_per_km")
    distance_km = float(run.get("distance_km") or 0.0)
    avg_hr = run.get("avg_hr")
    if not run_pace or run_pace <= 0 or marathon_pace_sec_per_km <= 0:
        return "unknown"

    ratio = run_pace / marathon_pace_sec_per_km
    hr_high = avg_hr is not None and avg_hr >= 0.9 * THRESHOLD_HR
    hr_moderate = avg_hr is not None and avg_hr >= 0.82 * THRESHOLD_HR

    if distance_km >= 18:
        if ratio <= 1.03 and hr_moderate:
            return "marathon_specific_long"
        if ratio <= 1.10:
            return "steady_long"
        return "easy_long"

    if distance_km >= 12:
        if ratio <= 0.95 or hr_high:
            return "tempo"
        if ratio <= 1.03:
            return "marathon_specific"
        if ratio <= 1.12:
            return "aerobic"
        return "easy"

    if distance_km >= 8:
        if ratio <= 0.92:
            return "speed"
        if ratio <= 0.97 or hr_high:
            return "tempo"
        if ratio <= 1.05:
            return "steady"
        if ratio <= 1.15:
            return "aerobic"
        return "easy"

    if ratio <= 1.05 and hr_moderate:
        return "steady"
    if ratio <= 1.15:
        return "easy"
    return "recovery"


def running_stress_score(activity, marathon_pace_sec_per_km):
    activity_type = activity["type"]
    distance_km = float(activity.get("distance_km") or 0.0)
    duration_minutes = float(activity.get("moving_time_sec") or 0.0) / 60.0
    elevation_gain = float(activity.get("elevation_gain") or 0.0)
    elevation_factor = 1.0 + min(0.12, elevation_gain / 5000.0)

    if activity_type in {"run", "trailrun"}:
        intensity = activity.get("intensity") or classify_run_intensity(activity, marathon_pace_sec_per_km)
        intensity_factor = RUN_INTENSITY_FACTOR.get(intensity, 1.0)
        if distance_km < 3.0:
            return 0.0
        return round(distance_km * intensity_factor * elevation_factor, 2)

    type_factor = STRESS_TYPE_FACTOR.get(activity_type, 0.5)
    if distance_km > 0:
        return round(distance_km * type_factor * elevation_factor, 2)
    if duration_minutes > 0:
        return round((duration_minutes / 10.0) * type_factor, 2)
    return 0.0


def load_model(load_activities, today, marathon_pace_sec_per_km, days=14):
    if load_activities:
        earliest = min(a["date"] for a in load_activities)
        start = min(earliest, today - timedelta(days=120))
    else:
        start = today - timedelta(days=120)

    daily_tss = {}
    for activity in load_activities:
        stress = running_stress_score(activity, marathon_pace_sec_per_km)
        daily_tss[activity["date"]] = daily_tss.get(activity["date"], 0.0) + stress

    ctl = 0.0
    atl = 0.0
    ctl_timeline = {}
    atl_timeline = {}
    day = start
    while day <= today:
        tss = daily_tss.get(day, 0.0)
        ctl = ctl + (tss - ctl) / 42.0
        atl = atl + (tss - atl) / 7.0
        ctl_timeline[day] = round(ctl, 1)
        atl_timeline[day] = round(atl, 1)
        day += timedelta(days=1)

    series_start = today - timedelta(days=days - 1)
    ctl_out = []
    atl_out = []
    for i in range(days):
        day = series_start + timedelta(days=i)
        ctl_out.append({"date": day.isoformat(), "label": f"{day.strftime('%b')} {day.day}", "value": ctl_timeline.get(day, 0.0)})
        atl_out.append({"date": day.isoformat(), "label": f"{day.strftime('%b')} {day.day}", "value": atl_timeline.get(day, 0.0)})

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
