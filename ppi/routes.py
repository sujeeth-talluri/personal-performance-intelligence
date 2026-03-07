from datetime import date

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for

from .repositories import (
    create_athlete,
    ensure_default_athlete,
    fetch_active_goal,
    fetch_all_athletes,
    fetch_athlete,
    fetch_recent_metrics,
    insert_goal,
)
from .services.ai_recommendation_service import generate_ai_recommendation
from .services.analytics_service import (
    build_goal_context,
    estimate_race_projection,
    get_recent_activity_summaries,
    goal_snapshot,
    live_training_state,
    milestone_progress,
    next_few_weeks_plan,
    rule_based_recommendation,
    today_focus,
    tomorrow_activity,
)
from .services.strava_oauth_service import (
    exchange_code_for_token,
    generate_oauth_state,
    get_authorize_url,
    link_oauth_identity,
)
from .services.strava_service import sync_athlete_from_strava

web = Blueprint("web", __name__)


def _resolve_athlete_id(raw_id):
    athletes = fetch_all_athletes()
    if not athletes:
        return ensure_default_athlete()

    if raw_id:
        try:
            athlete_id = int(raw_id)
            if fetch_athlete(athlete_id):
                return athlete_id
        except ValueError:
            pass

    return athletes[0]["athlete_id"]


@web.route("/", methods=["GET"])
def dashboard():
    ensure_default_athlete()
    athlete_id = _resolve_athlete_id(request.args.get("athlete_id"))

    sync_info = None
    sync_error = None
    try:
        sync_info = sync_athlete_from_strava(
            athlete_id=athlete_id,
            pages=current_app.config.get("STRAVA_FETCH_PAGES", 3),
        )
    except Exception as exc:
        sync_error = str(exc)

    goal = fetch_active_goal(athlete_id)
    if not goal:
        return redirect(url_for("web.setup", athlete_id=athlete_id))

    goal_context = build_goal_context(goal)
    intel = estimate_race_projection(athlete_id, goal_context)
    milestone = milestone_progress(athlete_id)
    live_state = live_training_state(athlete_id)
    recent_metrics = fetch_recent_metrics(athlete_id, limit=14)
    last_three_activities = get_recent_activity_summaries(athlete_id, limit=3)

    recommendation = generate_ai_recommendation(
        goal=goal_context,
        intel=intel,
        milestone=milestone,
        recent_metrics=recent_metrics,
    )
    fallback_reco = rule_based_recommendation(intel)
    tomorrow_plan = tomorrow_activity(intel, live_state["activity_done_today"])

    return render_template(
        "dashboard.html",
        athletes=fetch_all_athletes(),
        athlete_id=athlete_id,
        goal=goal_context,
        intel=intel,
        milestone=milestone,
        live_state=live_state,
        recommendation=recommendation,
        fallback_reco=fallback_reco,
        tomorrow_plan=tomorrow_plan,
        today_message=today_focus(live_state, intel),
        goal_snapshot=goal_snapshot(goal_context, intel, milestone),
        next_weeks_plan=next_few_weeks_plan(intel),
        last_three_activities=last_three_activities,
        today_date=date.today().isoformat(),
        sync_info=sync_info,
        sync_error=sync_error,
    )


@web.route("/athletes", methods=["POST"])
def add_athlete():
    name = request.form.get("name", "").strip()
    if not name:
        return redirect(url_for("web.dashboard"))

    athlete_id = create_athlete(name)
    return redirect(url_for("web.setup", athlete_id=athlete_id))


@web.route("/setup", methods=["GET", "POST"])
def setup():
    ensure_default_athlete()
    athlete_id = _resolve_athlete_id(request.args.get("athlete_id"))

    if request.method == "POST":
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation = request.form.get("elevation", "flat").strip()

        try:
            distance = float(request.form.get("distance", "0"))
        except ValueError:
            distance = 0

        if race_name and race_date and goal_time and distance > 0:
            insert_goal(
                athlete_id=athlete_id,
                event_name=race_name,
                distance_km=distance,
                goal_time=goal_time,
                race_date=race_date,
                elevation_type=elevation,
            )
            return redirect(url_for("web.dashboard", athlete_id=athlete_id))

    return render_template("setup.html", athletes=fetch_all_athletes(), athlete_id=athlete_id)


@web.route("/auth/strava/login", methods=["GET"])
def strava_login():
    athlete_id = _resolve_athlete_id(request.args.get("athlete_id"))
    state = generate_oauth_state()
    session["strava_state"] = state
    session["oauth_athlete_id"] = athlete_id
    return redirect(get_authorize_url(state))


@web.route("/auth/strava/callback", methods=["GET"])
def strava_callback():
    expected = session.get("strava_state")
    received = request.args.get("state")
    if not expected or expected != received:
        return redirect(url_for("web.dashboard"))

    code = request.args.get("code")
    athlete_id = session.get("oauth_athlete_id")
    if not code or not athlete_id:
        return redirect(url_for("web.dashboard"))

    payload = exchange_code_for_token(code)
    link_oauth_identity(int(athlete_id), payload)

    session.pop("strava_state", None)
    session.pop("oauth_athlete_id", None)

    return redirect(url_for("web.dashboard", athlete_id=athlete_id))
