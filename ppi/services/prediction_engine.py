def fit_half_equivalent(pace_medium, pace_long):
    if pace_medium and pace_long:
        return (0.55 * pace_long + 0.45 * pace_medium) * 21.1
    if pace_long:
        return pace_long * 21.1
    if pace_medium:
        return pace_medium * 21.1
    return None


def riegel_projection(time_seconds, distance_km, target_km=42.195, exponent=1.06):
    if not time_seconds or distance_km <= 0:
        return None
    return time_seconds * ((target_km / distance_km) ** exponent)


def vo2max_estimate_from_runs(runs):
    candidates = []
    for run in runs:
        distance = float(run.get("distance_km") or 0.0)
        moving = float(run.get("moving_time_sec") or 0.0)
        if distance < 5 or distance > 21.5 or moving <= 0:
            continue
        velocity_kmh = distance / (moving / 3600.0)
        candidates.append(3.5 + (velocity_kmh * 3.77))
    return round(max(candidates), 1) if candidates else None


def vo2max_marathon_projection(vo2max):
    if not vo2max or vo2max <= 3.5:
        return None
    marathon_velocity = (vo2max - 3.5) / 3.77
    if marathon_velocity <= 0:
        return None
    hours = 42.195 / marathon_velocity
    return hours * 3600.0


def marathon_prediction_seconds(metrics):
    candidate_signals = []
    half_equiv = fit_half_equivalent(metrics["pace_medium"], metrics["pace_long"])
    if half_equiv:
        candidate_signals.append((half_equiv * 2.1, 0.14))

    vo2_projection = vo2max_marathon_projection(metrics.get("vo2max_estimate"))
    if vo2_projection:
        candidate_signals.append((vo2_projection, 0.08))

    for run in metrics.get("recent_race_runs", []):
        projected = riegel_projection(run["moving_time_sec"], run["distance_km"])
        if projected:
            candidate_signals.append((projected, 0.26))

    for run in metrics.get("medium_runs", []):
        if 10 <= run["distance_km"] <= 30:
            intensity = run.get("intensity")
            weight = 0.12 if intensity in {"tempo", "speed"} else 0.10 if intensity in {"marathon_specific", "steady"} else 0.06
            exponent = 1.05 if intensity in {"tempo", "speed"} else 1.06
            candidate_signals.append((run["moving_time_sec"] * ((42.195 / run["distance_km"]) ** exponent), weight))

    for run in metrics.get("marathon_specific_runs", []):
        if run["distance_km"] >= 14 and run.get("pace_sec_per_km"):
            exponent = 1.03 if run.get("intensity") in {"marathon_specific_long", "steady_long"} else 1.04
            weight = 0.32 if run["distance_km"] >= 22 else 0.26
            candidate_signals.append((run["moving_time_sec"] * ((42.195 / run["distance_km"]) ** exponent), weight))

    for run in metrics.get("race_simulation_runs", []):
        candidate_signals.append((run["moving_time_sec"] * ((42.195 / run["distance_km"]) ** 1.03), 0.36))

    for run in metrics.get("long_runs", []):
        if run["distance_km"] >= 24 and run.get("pace_sec_per_km"):
            marathon_pace = metrics.get("goal_marathon_pace_sec_per_km") or run["pace_sec_per_km"]
            pace_gap = abs(run["pace_sec_per_km"] - marathon_pace) / max(1.0, marathon_pace)
            if pace_gap <= 0.15:
                weight = 0.28 if run.get("intensity") == "marathon_specific_long" else 0.20
                exponent = 1.03 if run.get("intensity") == "marathon_specific_long" else 1.05
                candidate_signals.append((run["moving_time_sec"] * ((42.195 / run["distance_km"]) ** exponent), weight))

    if not candidate_signals:
        return None

    weight_total = sum(weight for _, weight in candidate_signals)
    marathon_time = sum(value * weight for value, weight in candidate_signals) / max(1e-6, weight_total)

    fri = metrics["endurance"]["fri"]
    if fri is None:
        fatigue_factor = 1.08
    elif fri > 0.97:
        fatigue_factor = 1.02
    elif fri >= 0.94:
        fatigue_factor = 1.05
    elif fri >= 0.90:
        fatigue_factor = 1.07
    else:
        fatigue_factor = 1.10

    marathon_time *= fatigue_factor

    adi = metrics["endurance"].get("adi")
    if adi is not None:
        if adi <= 5:
            marathon_time *= 0.99
        elif adi > 8:
            marathon_time *= 1.03

    weekly = metrics["weekly"]["prior_avg_km"] or metrics["weekly"]["completed_km"]
    if weekly >= 65:
        marathon_time *= 0.98
    elif weekly < 45:
        marathon_time *= 1.03

    if metrics.get("rebuild_mode"):
        marathon_time *= 1.05
    tsb = metrics.get("tsb_proxy", 0.0)
    fatigue_ratio = metrics.get("fatigue_ratio", 1.0)
    if tsb < -20 or fatigue_ratio >= 1.35:
        marathon_time *= 1.03
    elif tsb > 2 and weekly >= 55:
        marathon_time *= 0.99
    if metrics.get("phase") == "recovery":
        marathon_time *= 1.01
    if metrics.get("phase") == "taper":
        marathon_time *= 0.99

    return marathon_time
