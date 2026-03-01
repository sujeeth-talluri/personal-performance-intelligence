BASELINE_RHR = 48

def calculate_readiness(tsb, resting_hr):
    score = 70

    if tsb < -20:
        score = 35
    elif -20 <= tsb < -10:
        score = 50
    elif -10 <= tsb <= 5:
        score = 70
    elif 5 < tsb <= 15:
        score = 85
    else:
        score = 70

    rhr_deviation = resting_hr - BASELINE_RHR

    if rhr_deviation >= 5:
        score -= 15
    elif rhr_deviation >= 2:
        score -= 7

    score = max(0, min(score, 100))

    return score