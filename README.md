# StrideIQ (Performance Intelligence Platform)

Runner-friendly goal tracking app with Strava sync, race prediction, and daily/weekly training guidance.

## What Runners See

- Clear race goal and days left
- "Where you are now" vs goal time
- What to do today and tomorrow
- Next 4 weeks plan in plain language
- Last 3 activities
- Weekly distance (Monday to Sunday)

## Local Run

```bash
python app.py
```

Open: `http://localhost:5000`

## Required Environment Variables

- `CLIENT_ID` (Strava app client id)
- `CLIENT_SECRET` (Strava app secret)
- `STRAVA_REDIRECT_URI` (for local: `http://localhost:5000/auth/strava/callback`)

Optional:

- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `STRAVA_FETCH_PAGES`
- `SECRET_KEY`
- `PPI_DB_PATH`

## Deploy Online (Friends Access)

### Render (ready files included)

This repo includes:

- `render.yaml`
- `Procfile`
- `runtime.txt`

Deploy steps:

1. Push repo to GitHub
2. In Render: New + Blueprint
3. Select this repo
4. Set env vars in Render dashboard:
   - `CLIENT_ID`
   - `CLIENT_SECRET`
   - `STRAVA_REDIRECT_URI` (must be your Render HTTPS domain callback)
   - `OPENAI_API_KEY` (optional)
5. Deploy and open your URL

### Generic Python hosting

```bash
gunicorn wsgi:application --bind 0.0.0.0:8000 --workers 2 --threads 4 --timeout 120
```

Important for Strava OAuth:

1. Your app must be served on HTTPS
2. Update `STRAVA_REDIRECT_URI` to your public domain callback (example: `https://yourdomain.com/auth/strava/callback`)
3. Set same callback domain in Strava developer app settings

## Android App Distribution

### Fastest way now (no Play Store)

1. Host online on HTTPS
2. Open in Android Chrome
3. Tap "Add to Home screen"
4. App installs as a standalone experience (PWA)

### Play Store path

Use Trusted Web Activity (TWA):

- Full guide: [docs/android-twa.md](docs/android-twa.md)

## Tests

```bash
pytest -q
```
