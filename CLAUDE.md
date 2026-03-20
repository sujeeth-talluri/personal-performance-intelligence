# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run locally
python app.py                      # starts Flask dev server at http://localhost:5000

# Tests
pytest -q                          # run all tests
pytest tests/test_app.py -q        # route/auth/plan tests only
pytest tests/test_service_engines.py -q  # service logic tests only
pytest -k "test_long_run" -q       # run a single test by name

# Production server
gunicorn wsgi:application --bind 0.0.0.0:8000 --workers 2 --threads 4 --timeout 120
```

Tests use SQLite in a temp directory (no env vars required). The production app requires `SECRET_KEY` and `DATABASE_URL` — `create_app()` raises `RuntimeError` without them, so local scripts that import the app must use `TestConfig` (see `tests/test_app.py`).

## Architecture

### Request flow

```
app.py / wsgi.py
  └─ ppi/__init__.py   create_app()  — Flask app factory; registers Blueprint, lazy-inits schema
       └─ ppi/routes.py              — single Blueprint "web"; all HTTP routes live here
            ├─ ppi/repositories.py   — SQLAlchemy queries; only layer that touches DB directly
            └─ ppi/services/         — pure business logic, no HTTP or DB imports
```

### Service layer breakdown

| File | Responsibility |
|---|---|
| `analytics_service.py` | Master aggregator. Calls all engines and assembles the `performance_intelligence()` dict that powers the dashboard JSON. Also owns `_long_run_progress_state`, `_training_phase`, `recent_runs`. |
| `prediction_engine.py` | Jack Daniels VDOT model for race predictions. Contains `predict_all_distances`, `marathon_prediction_seconds`, `marathon_wall_analysis`, `fm_wall_prevention_pace_strategy`, `marathon_fueling_plan`. Hardcoded `PB_FLOORS` act as fitness floor anchors — update these when the athlete sets new PRs. |
| `load_engine.py` | TSS / CTL / ATL / TSB computation. Zone classification uses threshold pace derived from goal marathon pace. |
| `plan_engine.py` | Weekly training plan template generation. `LONG_RUN_LADDER = [21, 24, 28, 32]` must stay in sync with the same constant in `analytics_service._long_run_progress_state`. `_next_long_run_target(apply_capacity_cap=False)` is called from the template builder to skip capacity checks. |
| `ai_recommendation_service.py` | Deterministic coaching sections (race prediction, pace strategy, training recs) assembled first; OpenAI used only for the short natural-language coaching paragraph. `generate_coaching_output` is the main entry point. |
| `strava_service.py` | Strava API fetch and activity sync. |

### Data models (`ppi/models.py`)

- `User` / `Goal` — one active goal per user (latest by `id`); `Goal` stores per-distance PBs (`pb_5k`, `pb_10k`, `pb_hm`).
- `Activity` — synced from Strava; unique on `(user_id, strava_activity_id)`.
- `Metric` — daily CTL/ATL/TSB snapshot; unique on `(user_id, date)`.
- `WorkoutLog` — weekly plan rows; unique on `(user_id, workout_date)`; `source` distinguishes `"engine"` (auto-generated) vs `"user"` (manual). Engine rows with `status="planned"` are overwritten on every `_build_weekly_plan` call — this is the self-healing mechanism for stale plan data.

### Key invariants

- **VDOT anchor**: `_BEST_PB_FLOOR_VDOT = 44.78` (driven by HM PB of 1:40:40). All distance predictions floor to this VDOT. Update `PB_FLOORS` in `prediction_engine.py` when new PRs are set.
- **Long run ladder**: `[21, 24, 28, 32]` km — identical in both `plan_engine.LONG_RUN_LADDER` and `analytics_service._long_run_progress_state`. Changing one requires changing the other.
- **Ladder threshold**: strictly `>=` (no 95% fuzzy threshold); 63-day lookback; `last_long` picked by max distance not max date.
- **Longest run for capacity**: uses a separate 84-day (`runs_12w`) window; all other 8-week metrics use 56-day (`runs_8w`).
- **Pace strategy**: uses `int(goal_pace_sec_per_km)` to avoid float drift across segment arithmetic.
- **Gel schedule**: cap is `km >= 42.0` (not `> 41.0`) so gels near the finish line (e.g., km 41.8) are included.
- **TSB label**: displayed as "Form" in the UI with a tooltip; subtitle is dynamic ("Fresh" / "Neutral" / "Tired").

### Schema migration

Handled inline in `create_app()` via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` — no migration framework. New columns must be added there.

### Deployment

Deployed on Render. `render.yaml`, `Procfile`, and `runtime.txt` are included. Push to `main` triggers auto-deploy. Render deploy lag (~1–2 min) is normal after a push.
