from dotenv import load_dotenv
load_dotenv()

from auth import get_access_token
from strava_api import fetch_activities
from analytics import calculate_stress, calculate_atl_ctl
from readiness import calculate_readiness
from database import initialize_database, save_metrics, fetch_latest_metric, fetch_all_metrics
import time
from datetime import datetime

# --- CONFIG ---
import os

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

# --- INIT DATABASE ---
initialize_database()

# --- AUTH ---
access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)

# --- CHECK LAST STORED STATE ---
last_row = fetch_latest_metric()
after_timestamp = None

if last_row:
    last_activity_id, last_timestamp, last_stress, last_atl, last_ctl, last_tsb, last_readiness = last_row
    print(f"\nLast stored activity ID in DB: {last_activity_id}")

    starting_atl = last_atl
    starting_ctl = last_ctl

    last_datetime_obj = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    after_timestamp = int(time.mktime(last_datetime_obj.timetuple())) + 1
else:
    print("\nNo previous data found. Starting fresh.")
    starting_atl = 0
    starting_ctl = 0

# --- FETCH DATA (AFTER timestamp now exists) ---
activities = fetch_activities(access_token, pages=3, after_timestamp=after_timestamp)
if last_row and activities:
    last_activity_id, _, _, _, _, _, _ = last_row
    newest_fetched_id = sorted(
        activities,
        key=lambda x: x["start_date"]
    )[-1]["id"]

    if newest_fetched_id == last_activity_id:
        print("\nNo new activities since last sync.")
        exit()
if not activities:
    print("\nNo new activities since last sync.")
    exit()

print(f"\nTotal activities fetched: {len(activities)}")

# --- ANALYTICS ---
stress_data, distance_data = calculate_stress(activities)
metrics = calculate_atl_ctl(stress_data, starting_atl, starting_ctl)

from collections import defaultdict
from datetime import date

# --- Calculate Mileage ---
weekly_km = 0
monthly_km = 0

today = date.today()
current_year = today.year
current_month = today.month
current_week = today.isocalendar()[1]

for run_date, km in distance_data:
    if run_date.year == current_year:
        if run_date.month == current_month:
            monthly_km += km
        if run_date.isocalendar()[1] == current_week:
            weekly_km += km

weekly_km = round(weekly_km, 1)
monthly_km = round(monthly_km, 1)

print("\n--- Mileage Summary ---")
print(f"Weekly KM: {weekly_km}")
print(f"Monthly KM: {monthly_km}")

# --- GET LATEST DATE ---
# Identify latest activity from fetched list
latest_activity = sorted(
    activities,
    key=lambda x: x["start_date"]
)[-1]

latest_activity_id = latest_activity["id"]
latest_timestamp = latest_activity["start_date"]

# Extract metrics using date
from datetime import datetime

latest_date_obj = datetime.strptime(
    latest_timestamp,
    "%Y-%m-%dT%H:%M:%SZ"
).date()

latest_metrics = metrics[latest_date_obj]

print("\nLatest Performance Metrics:")
print(f"Activity ID: {latest_activity_id}")
print(f"Timestamp: {latest_timestamp}")
print(f"Stress: {latest_metrics['stress']:.1f}")
print(f"ATL: {latest_metrics['atl']:.1f}")
print(f"CTL: {latest_metrics['ctl']:.1f}")
print(f"TSB: {latest_metrics['tsb']:.1f}")

# --- READINESS ---
resting_hr_today = int(input("\nEnter today's resting HR: "))
readiness_score = calculate_readiness(latest_metrics["tsb"], resting_hr_today)

print(f"\nFinal Readiness Score: {readiness_score}")

# --- SAVE TO DATABASE ---
stress = round(latest_metrics["stress"], 1)
atl = round(latest_metrics["atl"], 1)
ctl = round(latest_metrics["ctl"], 1)
tsb = round(latest_metrics["tsb"], 1)

save_metrics(
    latest_activity_id,
    latest_timestamp,
    stress,
    atl,
    ctl,
    tsb,
    readiness_score
)

print("\nMetrics saved to database successfully.")
print("\nStored Metrics in Database:\n")

rows = fetch_all_metrics()

for row in rows:
    print(row)