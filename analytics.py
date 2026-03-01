from datetime import datetime
from collections import defaultdict

THRESHOLD_HR = 168


def calculate_stress(activities):
    stress_data = []
    distance_data = []

    for activity in activities:
        date_str = activity.get("start_date")
        duration_min = activity.get("moving_time", 0) / 60
        avg_hr = activity.get("average_heartrate")
        distance_km = activity.get("distance", 0) / 1000

        if avg_hr:
            intensity_factor = avg_hr / THRESHOLD_HR
            stress = duration_min * intensity_factor
        else:
            stress = 0

        date_obj = datetime.strptime(
            date_str,
            "%Y-%m-%dT%H:%M:%SZ"
        ).date()

        stress_data.append((date_obj, stress))
        distance_data.append((date_obj, distance_km))

    return stress_data, distance_data


def calculate_atl_ctl(stress_data, starting_atl=0, starting_ctl=0):
    daily_stress = defaultdict(float)

    for date, stress in stress_data:
        daily_stress[date] += stress

    sorted_dates = sorted(daily_stress.keys())

    atl = starting_atl
    ctl = starting_ctl

    metrics = {}

    for date in sorted_dates:
        stress = daily_stress[date]

        atl = atl + (stress - atl) * (1/7)
        ctl = ctl + (stress - ctl) * (1/42)
        tsb = ctl - atl

        metrics[date] = {
            "stress": stress,
            "atl": atl,
            "ctl": ctl,
            "tsb": tsb
        }

    return metrics