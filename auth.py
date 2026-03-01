import requests

def get_access_token(client_id, client_secret, refresh_token):
    token_url = "https://www.strava.com/oauth/token"

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = requests.post(token_url, data=payload)

    if response.status_code != 200:
        raise Exception(f"Token refresh failed: {response.json()}")

    return response.json()["access_token"]