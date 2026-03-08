from functools import wraps

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .repositories import (
    create_user,
    get_goal,
    get_user_by_email,
    get_user_by_id,
    save_goal,
    update_user_name,
)
from .services.analytics_service import (
    build_goal_context,
    long_run_progress,
    performance_intelligence,
    recent_runs,
    today_training,
    weekly_training,
)
from .services.strava_oauth_service import (
    exchange_code_for_token,
    generate_oauth_state,
    get_authorize_url,
    link_oauth_identity,
)
from .services.strava_service import sync_strava_data

web = Blueprint("web", __name__)


def _current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return get_user_by_id(user_id)


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _current_user():
            return redirect(url_for("web.login"))
        return fn(*args, **kwargs)

    return wrapper


@web.route("/register", methods=["GET", "POST"])
def register():
    error = None
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not name or not email or len(password) < 6:
            error = "Please enter valid details (password min 6 chars)."
        elif get_user_by_email(email):
            error = "Email already exists. Please login."
        else:
            user_id = create_user(name, email, generate_password_hash(password))
            session["user_id"] = user_id
            return redirect(url_for("web.onboarding"))

    return render_template("register.html", error=error)


@web.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        user = get_user_by_email(email)
        if not user or not check_password_hash(user["password_hash"], password):
            error = "Invalid email or password"
        else:
            session["user_id"] = user["id"]
            return redirect(url_for("web.dashboard"))

    return render_template("login.html", error=error)


@web.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("web.login"))


@web.route("/")
@login_required
def dashboard():
    user = _current_user()

    sync_info = sync_strava_data(user_id=user["id"], pages=current_app.config.get("STRAVA_FETCH_PAGES", 3))

    goal = get_goal(user["id"])
    if not goal:
        return redirect(url_for("web.onboarding"))

    goal_context = build_goal_context(goal)
    intel = performance_intelligence(user["id"], goal_context)

    return render_template(
        "dashboard.html",
        user=user,
        goal=goal_context,
        intel=intel,
        long_run=long_run_progress(user["id"]),
        today_plan=today_training(intel),
        weekly=weekly_training(user["id"]),
        runs=recent_runs(user["id"], limit=5),
        sync_info=sync_info,
    )


@web.route("/onboarding", methods=["GET", "POST"])
@login_required
def onboarding():
    user = _current_user()

    if request.method == "POST":
        runner_name = request.form.get("runner_name", "").strip()
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "flat").strip()
        current_pb = request.form.get("current_pb", "").strip()

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        if runner_name and race_name and race_date and goal_time and race_distance > 0:
            update_user_name(user["id"], runner_name)
            save_goal(
                user_id=user["id"],
                race_name=race_name,
                race_distance=race_distance,
                goal_time=goal_time,
                race_date=race_date,
                elevation_type=elevation_type,
                current_pb=current_pb,
            )
            return redirect(url_for("web.dashboard"))

    return render_template("onboarding.html", user=user, goal=get_goal(user["id"]))


@web.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    user = _current_user()
    goal = get_goal(user["id"])

    if request.method == "POST":
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "flat").strip()
        current_pb = request.form.get("current_pb", "").strip()
        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        if race_name and race_date and goal_time and race_distance > 0:
            save_goal(
                user_id=user["id"],
                race_name=race_name,
                race_distance=race_distance,
                goal_time=goal_time,
                race_date=race_date,
                elevation_type=elevation_type,
                current_pb=current_pb,
            )
            return redirect(url_for("web.dashboard"))

    return render_template("settings.html", user=user, goal=goal)


@web.route("/connect/strava")
@login_required
def strava_login():
    user = _current_user()
    state = generate_oauth_state()
    session["strava_state"] = state
    session["oauth_user_id"] = user["id"]
    return redirect(get_authorize_url(state))


@web.route("/auth/strava/callback")
def strava_callback():
    expected = session.get("strava_state")
    received = request.args.get("state")
    if not expected or expected != received:
        return redirect(url_for("web.login"))

    user_id = session.get("oauth_user_id")
    code = request.args.get("code")
    if not user_id or not code:
        return redirect(url_for("web.login"))

    payload = exchange_code_for_token(code)
    link_oauth_identity(user_id, payload)

    session["user_id"] = user_id
    session.pop("strava_state", None)
    session.pop("oauth_user_id", None)

    if not get_goal(user_id):
        return redirect(url_for("web.onboarding"))
    return redirect(url_for("web.dashboard"))
