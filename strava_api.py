import requests

def fetch_activities(access_token, pages=3, after_timestamp=None):
    activities_url = "https://www.strava.com/api/v3/athlete/activities"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    all_activities = []

    for page in range(1, pages + 1):
        params = {
            "per_page": 30,
            "page": page
        }

        if after_timestamp:
            params["after"] = after_timestamp

        response = requests.get(activities_url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch activities: {response.json()}")

        activities = response.json()

        if not activities:
            break

        all_activities.extend(activities)

    return all_activities