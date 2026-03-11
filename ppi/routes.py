import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from functools import wraps
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import Blueprint, current_app, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .repositories import (
    consume_password_reset,
    create_password_reset,
    create_user,
    get_goal,
    get_password_reset,
    get_user_by_email,
    get_user_by_id,
    save_goal,
    update_password,
    update_user_name,
)
from .services.analytics_service import (
    performance_intelligence,
    recent_runs,
    today_training_reco,
    weekly_training_summary,
)
from .services.strava_oauth_service import (
    exchange_code_for_token,
    generate_oauth_state,
    get_authorize_url,
    link_oauth_identity,
)
from .services.strava_service import sync_strava_data

web = Blueprint("web", __name__)


def _utcnow_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _user_timezone_name():
    return current_app.config.get("USER_TIMEZONE") or current_app.config.get("APP_TIMEZONE") or "Asia/Kolkata"

def _today_date_label(user_timezone):
    try:
        tz = ZoneInfo(user_timezone)
    except ZoneInfoNotFoundError:
        fallback = {
            "asia/kolkata": timezone(timedelta(hours=5, minutes=30)),
            "asia/calcutta": timezone(timedelta(hours=5, minutes=30)),
            "utc": timezone.utc,
            "etc/utc": timezone.utc,
        }
        tz = fallback.get((user_timezone or "").lower(), timezone.utc)
    return datetime.now(timezone.utc).astimezone(tz).strftime("%b %d, %Y")

def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _should_sync_now(last_sync_at, cooldown_min):
    if not last_sync_at:
        return True
    return (datetime.now(timezone.utc) - last_sync_at) >= timedelta(minutes=cooldown_min)



def _today_local_date(user_timezone):
    try:
        tz = ZoneInfo(user_timezone)
    except ZoneInfoNotFoundError:
        tz = timezone.utc
    now_local = datetime.now(timezone.utc).astimezone(tz)
    return now_local.date()


def _week_bounds(today_local):
    start = today_local - timedelta(days=today_local.weekday())
    end = start + timedelta(days=6)
    return start, end


def _build_weekly_plan(today_local, runs, weekly_goal, long_run):
    week_start, _ = _week_bounds(today_local)
    run_by_date = {r["date"]: r for r in runs}

    long_km = int(round(long_run.get("next_milestone_km") or 24))
    easy_km = max(6, int(round((weekly_goal.get("weekly_goal_km", 60) * 0.14))))
    aerobic_km = max(8, int(round((weekly_goal.get("weekly_goal_km", 60) * 0.16))))
    tempo_km = max(10, int(round((weekly_goal.get("weekly_goal_km", 60) * 0.2))))

    schedule = [
        ("Mon", "Easy Run", f"{easy_km} km"),
        ("Tue", "Aerobic Run", f"{aerobic_km} km"),
        ("Wed", "Strength", "Gym"),
        ("Thu", "Tempo Run", f"{tempo_km} km"),
        ("Fri", "Strength", "Gym"),
        ("Sat", "Easy Run", f"{easy_km} km"),
        ("Sun", "Long Run", f"{long_km} km"),
    ]
    weekday_map = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}

    items = []
    for day_code, session_name, planned in schedule:
        day_date = week_start + timedelta(days=weekday_map[day_code])
        day_iso = day_date.isoformat()
        done_run = run_by_date.get(day_iso)

        planned_km = None
        if planned.endswith(" km"):
            try:
                planned_km = float(planned.split(" ")[0])
            except ValueError:
                planned_km = None

        is_distance_session = planned.endswith(" km")
        status = "upcoming"
        status_label = "Upcoming"
        if done_run:
            done_km = float(done_run["distance"])
            if session_name == "Rest" or (planned_km is not None and done_km > planned_km + 0.4):
                status = "extra"
                status_label = f"Extra {done_run['distance']} km"
            else:
                status = "completed"
                status_label = f"Completed {done_run['distance']} km"
        elif day_date < today_local and is_distance_session:
            status = "skipped"
            status_label = "Skipped"
        elif session_name == "Rest":
            status = "rest"
            status_label = "Rest"

        items.append(
            {
                "day": day_code,
                "date": day_date,
                "session": session_name,
                "planned": planned,
                "done": bool(done_run),
                "actual": f"{done_run['distance']} km" if done_run else None,
                "status": status,
                "status_label": status_label,
                "planned_km": planned_km,
                "workout_type": "RUN" if planned.endswith(" km") else "STRENGTH" if session_name == "Strength" else "REST",
            }
        )
    return items


def _next_key_workout_label(today_local, weekly_plan):
    for item in weekly_plan:
        if item["session"] == "Rest":
            continue
        if item["date"] > today_local and item["status"] not in {"completed", "extra"}:
            return f"{item['day']} - {item['planned']} {item['session']}"
    return "Complete this week's planned key sessions."


def _build_today_workout(today_local, runs, today_plan, next_key_workout):
    today_iso = today_local.isoformat()
    today_run = next((r for r in runs if r["date"] == today_iso), None)

    if today_run:
        return {
            "date": today_local.strftime("%a %b %d"),
            "workout": "Run",
            "status": "Completed",
            "distance": f"{today_run['distance']} km",
            "pace": today_run["pace"],
            "hr": str(today_run["hr"]) if today_run["hr"] else "--",
            "coach_insight": "Good aerobic execution today. Keep effort controlled.",
            "next_key_workout": next_key_workout,
            "completed": True,
        }

    return {
        "date": today_local.strftime("%a %b %d"),
        "workout": today_plan["title"],
        "status": "Upcoming",
        "distance": today_plan["details"],
        "pace": "--",
        "hr": "--",
        "coach_insight": "Today is a setup session to support marathon consistency.",
        "next_key_workout": next_key_workout,
        "completed": False,
    }


def _build_ai_summary(intel, weekly_plan):
    # Priority: injury risk -> missed workouts -> long run progression -> training load -> readiness
    if (intel.get("endurance", {}).get("lrr_status") or "").lower() == "fatigue risk":
        return "Injury risk is elevated. Reduce long-run strain and keep easy days truly easy."

    missed = [p for p in weekly_plan if p.get("status") == "skipped" and p.get("workout_type") == "RUN"]
    if missed:
        return "You missed a key run this week. Recover consistency before adding intensity."

    long_run_count = intel.get("training_counts", {}).get("long_runs", 0)
    if long_run_count < 2:
        return "Long run progression is behind. Schedule a long run this weekend to unlock prediction."

    trend = (intel.get("ctl_trend_text") or "").lower()
    if "down" in trend:
        return "Training load dropped recently. Add steady volume to stabilize fitness."

    next_req = intel.get("training_status", {}).get("next_requirement")
    if next_req:
        return f"Prediction readiness: {next_req}"

    return "Training is on track. Keep this week consistent and protect recovery."

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


def _send_reset_email(email, link):
    cfg = current_app.config
    if not cfg.get("SMTP_HOST"):
        print(f"[Password Reset Link] {email}: {link}")
        return

    msg = MIMEText(f"Reset your StrideIQ password: {link}")
    msg["Subject"] = "StrideIQ Password Reset"
    msg["From"] = cfg.get("SMTP_FROM")
    msg["To"] = email

    with smtplib.SMTP(cfg["SMTP_HOST"], cfg["SMTP_PORT"]) as server:
        server.starttls()
        if cfg.get("SMTP_USER") and cfg.get("SMTP_PASSWORD"):
            server.login(cfg["SMTP_USER"], cfg["SMTP_PASSWORD"])
        server.sendmail(msg["From"], [email], msg.as_string())


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
            try:
                user_id = create_user(name, email, generate_password_hash(password))
                session["user_id"] = user_id
                return redirect(url_for("web.onboarding"))
            except Exception:
                error = "Unable to create account right now. Please try again."

    return render_template("register.html", error=error)


@web.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        user = get_user_by_email(email)
        if not user or not check_password_hash(user.password_hash, password):
            error = "Invalid email or password"
        else:
            session["user_id"] = user.id
            return redirect(url_for("web.dashboard"))

    return render_template("login.html", error=error)


@web.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    message = None
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        user = get_user_by_email(email)
        if user:
            token = secrets.token_urlsafe(32)
            expires_at = _utcnow_naive() + timedelta(hours=1)
            create_password_reset(user.id, token, expires_at)
            link = url_for("web.reset_password", token=token, _external=True)
            _send_reset_email(email, link)

        message = "If this email exists, a password reset link has been sent."

    return render_template("forgot_password.html", message=message)


@web.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    row = get_password_reset(token)
    if not row or row.expires_at < _utcnow_naive():
        return redirect(url_for("web.login"))

    error = None
    if request.method == "POST":
        password = request.form.get("password", "")
        if len(password) < 6:
            error = "Password must be at least 6 characters."
        else:
            update_password(row.user_id, generate_password_hash(password))
            consume_password_reset(token)
            return redirect(url_for("web.login"))

    return render_template("reset_password.html", error=error)


@web.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("web.login"))


@web.route("/")
@login_required
def dashboard():
    user = _current_user()
    user_tz = _user_timezone_name()

    force_sync = request.args.get("sync") == "1"
    cooldown_min = int(current_app.config.get("STRAVA_SYNC_COOLDOWN_MIN", 15))
    last_sync_at = _parse_iso_datetime(session.get("last_sync_at"))

    if force_sync or _should_sync_now(last_sync_at, cooldown_min):
        sync_info = sync_strava_data(user_id=user.id, pages=current_app.config.get("STRAVA_FETCH_PAGES", 3))
        if sync_info.get("status") == "ok":
            session["last_sync_at"] = datetime.now(timezone.utc).isoformat()
    else:
        sync_info = {"status": "skipped", "reason": "cooldown", "new_activities": 0}

    intel = performance_intelligence(user.id, user_timezone=user_tz)

    if not intel or not intel.get("goal"):
        return redirect(url_for("web.onboarding"))

    weekly_summary = weekly_training_summary(user.id)
    weekly_goal = intel["weekly"]

    today_plan = today_training_reco(intel["probability"], user_timezone=user_tz)
    runs = recent_runs(user.id, limit=20, user_timezone=user_tz)
    today_local = _today_local_date(user_tz)

    weekly_plan = _build_weekly_plan(today_local, runs, weekly_goal, intel["long_run"])
    next_key_workout = _next_key_workout_label(today_local, weekly_plan)
    today_workout = _build_today_workout(today_local, runs, today_plan, next_key_workout)
    ai_summary = _build_ai_summary(intel, weekly_plan)
    key_session = next((item for item in weekly_plan if item["session"] == "Long Run"), weekly_plan[-1])
    key_session_importance = "High" if key_session["session"] == "Long Run" else "Medium" if key_session["session"] == "Tempo Run" else "Low"

    return render_template(
        "dashboard.html",
        user=user,
        goal=intel["goal"],
        intel=intel,
        weekly_goal=weekly_goal,
        weekly_summary=weekly_summary,
        endurance=intel["endurance"],
        today_plan=today_plan,
        show_today_plan=not today_workout["completed"],
        today_workout=today_workout,
        weekly_plan=weekly_plan,
        key_session=key_session,
        key_session_importance=key_session_importance,
        ai_summary=ai_summary,
        runs=runs[:5],
        sync_info=sync_info,
        today_date=_today_date_label(user_tz),
        long_run=intel["long_run"],
    )


@web.route("/onboarding", methods=["GET", "POST"])
@login_required
def onboarding():
    user = _current_user()
    error = request.args.get("error")

    if request.method == "POST":
        runner_name = request.form.get("runner_name", "").strip()
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "moderate").strip()
        personal_best = request.form.get("current_pb", "").strip()

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        if runner_name and race_name and race_date and goal_time and race_distance > 0:
            try:
                update_user_name(user.id, runner_name)
                save_goal(
                    user_id=user.id,
                    race_name=race_name,
                    race_distance=race_distance,
                    goal_time=goal_time,
                    race_date=race_date,
                    elevation_type=elevation_type,
                    personal_best=personal_best,
                )
                return redirect(url_for("web.dashboard"))
            except Exception:
                error = "Unable to save goal right now. Please try again."

    return render_template("onboarding.html", user=user, goal=get_goal(user.id), error=error)


@web.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    user = _current_user()
    goal = get_goal(user.id)
    error = request.args.get("error")

    if request.method == "POST":
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "moderate").strip()
        personal_best = request.form.get("current_pb", "").strip()

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        if race_name and race_date and goal_time and race_distance > 0:
            try:
                save_goal(
                    user_id=user.id,
                    race_name=race_name,
                    race_distance=race_distance,
                    goal_time=goal_time,
                    race_date=race_date,
                    elevation_type=elevation_type,
                    personal_best=personal_best,
                )
                return redirect(url_for("web.dashboard"))
            except Exception:
                error = "Unable to update settings right now. Please try again."

    return render_template("settings.html", user=user, goal=goal, error=error)


@web.route("/connect/strava")
@login_required
def strava_login():
    user = _current_user()
    state = generate_oauth_state()
    session["strava_state"] = state
    session["oauth_user_id"] = user.id
    try:
        return redirect(get_authorize_url(state))
    except Exception:
        current_app.logger.exception("Failed to start Strava OAuth")
        return redirect(url_for("web.onboarding", error="Strava OAuth is not configured. Set CLIENT_ID, CLIENT_SECRET and STRAVA_REDIRECT_URI in Render."))


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

    try:
        payload = exchange_code_for_token(code)
        link_oauth_identity(user_id, payload)
    except Exception:
        current_app.logger.exception("Strava OAuth callback failed")
        return redirect(url_for("web.onboarding", error="Strava authorization failed. Check CLIENT_ID/CLIENT_SECRET and callback URL settings."))

    session["user_id"] = user_id
    session.pop("strava_state", None)
    session.pop("oauth_user_id", None)

    if not get_goal(user_id):
        return redirect(url_for("web.onboarding"))
    return redirect(url_for("web.dashboard"))







