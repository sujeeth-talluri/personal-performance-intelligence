# Performance Intelligence Platform

## Architecture

- `app.py`: runtime entrypoint using Flask app factory
- `ppi/__init__.py`: app creation and wiring
- `ppi/db.py`: DB connection lifecycle + schema bootstrap
- `ppi/repositories.py`: storage abstraction (athletes, goals, metrics, Strava accounts)
- `ppi/services/analytics_service.py`: stress, ATL/CTL/TSB, live analytics, race projection model
- `ppi/services/strava_oauth_service.py`: Strava OAuth and token refresh lifecycle
- `ppi/services/strava_service.py`: automatic incremental activity sync
- `ppi/services/ai_recommendation_service.py`: AI coach recommendation with heuristic fallback
- `ppi/routes.py`: UI and auth routes
- `ppi/templates/*.html`: mobile-first dashboard/setup UI

## Core Flows

1. Multi-athlete management
   - Create athlete in dashboard
   - Select athlete from dropdown
   - Race goal stored per athlete

2. Strava OAuth login
   - `GET /auth/strava/login?athlete_id=...` redirects to Strava
   - `GET /auth/strava/callback` stores per-athlete tokens in `strava_accounts`

3. Automatic sync on dashboard load
   - `GET /` triggers incremental Strava sync for selected athlete
   - New activities are converted to stress/load/readiness and stored in `daily_metrics`

4. Real-time analytics
   - Dashboard computes live weekly load, milestones, CTL/TSB readiness
   - Race prediction uses Riegel baseline + load/fatigue/elevation adjustments

5. AI coaching
   - If `OPENAI_API_KEY` exists: LLM recommendation from athlete context
   - Else: deterministic fallback recommendation

## Environment Variables

- `SECRET_KEY`
- `PPI_DB_PATH`
- `CLIENT_ID`
- `CLIENT_SECRET`
- `REFRESH_TOKEN` (optional fallback, non-OAuth mode)
- `STRAVA_REDIRECT_URI` (default: `http://localhost:5000/auth/strava/callback`)
- `STRAVA_FETCH_PAGES` (default: `3`)
- `OPENAI_API_KEY` (optional)
- `OPENAI_MODEL` (optional, default `gpt-4.1-mini`)

## Run

```bash
python app.py
```

For production, run `wsgi.py` via Gunicorn/Uvicorn worker model behind a reverse proxy.
