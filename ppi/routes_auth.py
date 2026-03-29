"""
routes_auth.py
──────────────
Authentication, password reset, onboarding, settings, coach-intro, and
Strava OAuth routes — all registered on the shared `web` Blueprint that
is defined in routes.py.

Import pattern: `from .routes import web` pulls the already-constructed
Blueprint so all routes end up in one place for Flask registration.
"""
import json
import secrets
import smtplib
from datetime import date as _date, datetime, timedelta

from email.mime.text import MIMEText

import requests as http_requests
from flask import current_app, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .repositories import (
    consume_password_reset,
    create_password_reset,
    create_user,
    get_goal,
    get_password_reset,
    get_user_by_email,
    save_goal,
    update_password,
    update_user_name,
)
from .services.strava_oauth_service import (
    exchange_code_for_token,
    generate_oauth_state,
    get_authorize_url,
    link_oauth_identity,
)
from .models import Activity, Goal, RunnerProfile
from .extensions import csrf, db, limiter

# Import the shared blueprint and shared helpers from routes.py
from .routes import web, _current_user, login_required, _utcnow_naive


# ---------------------------------------------------------------------------
# Email helper
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@web.route("/register", methods=["GET", "POST"])
@limiter.limit("10 per hour; 3 per minute")
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
@limiter.limit("20 per hour; 5 per minute")
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
@limiter.limit("5 per hour")
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


@web.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("web.login"))


# ---------------------------------------------------------------------------
# Coach-intro (conversational AI onboarding)
# ---------------------------------------------------------------------------

_COACH_FALLBACKS = {
    0: {
        "text": "Welcome! I'm Coach Ike, your AI marathon coach. I have 4 quick questions to personalise your training plan. First — how consistently have you been running recently?",
        "options": ["Just getting started (< 1 month)", "Getting back into it (had a break)", "Training consistently (3+ months)", "Well trained (6+ months solid)"],
        "input_type": "options",
    },
    1: {
        "text": "Good to know. Have you completed this race distance before?",
        "options": ["First time at this distance", "Done it once before", "Done it multiple times"],
        "input_type": "options",
    },
    2: {
        "text": "Got it. Any injuries or niggles I should factor in from the last 6 months?",
        "options": ["Fully healthy — no issues", "Minor issue, mostly recovered", "Managing an ongoing issue"],
        "input_type": "options",
    },
    3: {
        "text": "Last one — what matters most to you about this race?",
        "options": ["Just finish healthy and strong", "Beat my previous time", "Hit my specific time goal", "Qualify for a major race"],
        "input_type": "options",
    },
}

_STEP_KEYS = [
    "consistency_level",
    "race_experience",
    "injury_status",
    "goal_priority",
]


def _get_next_coach_message(current_step, user_answer, collected, goal, user) -> dict:
    """Call Claude to generate the next conversational coaching message.
    Falls back to hardcoded questions if the API is unavailable."""
    dist_km = float(getattr(goal, "race_distance", None) or 42.2)
    if dist_km > 40:
        dist_label = "marathon"
    elif dist_km > 20:
        dist_label = "half marathon"
    elif dist_km > 9:
        dist_label = "10K"
    else:
        dist_label = "5K"

    prompt = (
        f"You are a warm, encouraging elite marathon coach named Coach Ike "
        f"having your first conversation with a new runner.\n\n"
        f"Runner's goal: {goal.goal_time} {dist_label} on {goal.race_date} ({goal.race_name})\n"
        f"Runner's name: {user.name or 'there'}\n\n"
        f"Conversation so far:\n{json.dumps(collected, indent=2)}\n\n"
        f"Current step: {current_step} of 3\n"
        f"Previous answer: \"{user_answer}\"\n\n"
        f"Generate the next conversational message for step {current_step}.\n\n"
        f"STEP DEFINITIONS (only 4 steps total — 0 through 3):\n"
        f"Step 0: Warm welcome using runner's name and their specific goal. Mention this is just 4 quick questions. Ask how consistently they have been running recently.\n"
        f"  Options: [\"Just getting started (< 1 month)\", \"Getting back into it (had a break)\", \"Training consistently (3+ months)\", \"Well trained (6+ months solid)\"]\n"
        f"Step 1: Brief acknowledgment. Ask if they have completed this distance before.\n"
        f"  Options: [\"First time at this distance\", \"Done it once before\", \"Done it multiple times\"]\n"
        f"Step 2: Brief acknowledgment. Ask about injuries or niggles in last 6 months.\n"
        f"  Options: [\"Fully healthy — no issues\", \"Minor issue, mostly recovered\", \"Managing an ongoing issue\"]\n"
        f"Step 3: Brief acknowledgment (if injury mentioned, show empathy). Ask what matters most to them about this race. This is the LAST question — end warmly.\n"
        f"  Options: [\"Just finish healthy and strong\", \"Beat my previous time\", \"Hit my specific time goal\", \"Qualify for a major race\"]\n\n"
        f"RULES:\n"
        f"- Keep each message SHORT — 1-2 sentences max before the question\n"
        f"- Be warm, personal, specific to their goal and race name\n"
        f"- If injury mentioned, acknowledge with care\n"
        f"- Never be generic\n\n"
        f"Respond ONLY with valid JSON (no markdown, no code fences):\n"
        f"{{\"text\": \"your message\", \"options\": [\"opt1\", \"opt2\", ...], \"input_type\": \"options\", \"collected\": {{...updated dict...}}}}"
    )

    api_key = None
    model = "claude-sonnet-4-5"
    try:
        api_key = current_app.config.get("ANTHROPIC_API_KEY")
        model = current_app.config.get("ANTHROPIC_MODEL", "claude-sonnet-4-5")
    except RuntimeError:
        pass

    if api_key:
        for _attempt in range(2):
            try:
                resp = http_requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": model,
                        "max_tokens": 500,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    raw = resp.json()["content"][0]["text"].strip()
                    raw = raw.replace("```json", "").replace("```", "").strip()
                    parsed = json.loads(raw)
                    if "text" in parsed and "options" in parsed:
                        return parsed
            except Exception:
                pass

    fallback = _COACH_FALLBACKS.get(current_step) or _COACH_FALLBACKS.get(len(_COACH_FALLBACKS) - 1, {})
    return {**fallback, "collected": collected}


def _save_runner_profile(user_id: int, collected: dict):
    """Persist collected onboarding answers into RunnerProfile."""
    consistency_map = {
        "Just getting started (< 1 month)": "just_starting",
        "Getting back into it (had a break)": "getting_back",
        "Training consistently (3+ months)": "consistent",
        "Well trained (6+ months solid)": "well_trained",
    }
    experience_map = {
        "First time at this distance": "first_time",
        "Done it once before": "once",
        "Done it multiple times": "multiple",
    }
    injury_map = {
        "Fully healthy — no issues": "healthy",
        "Minor issue, mostly recovered": "minor",
        "Managing an ongoing issue": "ongoing",
    }
    days_map = {"3 days": 3, "4 days": 4, "5 days": 5, "6 days": 6}
    long_run_map = {
        "Saturday": "saturday",
        "Sunday": "sunday",
        "Flexible — no preference": "flexible",
    }
    strength_map = {
        "No strength training": 0,
        "1 session per week": 1,
        "2 sessions per week": 2,
    }
    run_time_map = {
        "Early morning (before 7am)": "early_morning",
        "Morning (7-10am)": "morning",
        "Evening (after 5pm)": "evening",
        "Flexible — it varies": "flexible",
    }
    priority_map = {
        "Just finish healthy and strong": "finish_healthy",
        "Beat my previous time": "beat_previous",
        "Hit my specific time goal": "hit_time",
        "Qualify for a major race": "qualify",
    }

    profile = RunnerProfile.query.filter_by(user_id=user_id).first()
    if not profile:
        profile = RunnerProfile(user_id=user_id)

    profile.consistency_level      = consistency_map.get(collected.get("consistency_level"), "consistent")
    profile.race_experience        = experience_map.get(collected.get("race_experience"), "once")
    profile.injury_status          = injury_map.get(collected.get("injury_status"), "healthy")
    profile.training_days_per_week = days_map.get(collected.get("training_days_per_week"), 5)
    profile.long_run_day           = long_run_map.get(collected.get("long_run_day"), "sunday")
    profile.strength_days_per_week = strength_map.get(collected.get("strength_days_per_week"), 2)
    profile.preferred_run_time     = run_time_map.get(collected.get("preferred_run_time"), "flexible")
    profile.goal_priority          = priority_map.get(collected.get("goal_priority"), "hit_time")
    profile.onboarding_completed   = True
    profile.completed_at           = _utcnow_naive()

    db.session.add(profile)
    db.session.commit()


@web.route("/coach-intro", methods=["GET"])
@login_required
def coach_intro():
    """Conversational AI onboarding — runs once, between goal setup and dashboard."""
    user = _current_user()

    profile = RunnerProfile.query.filter_by(user_id=user.id).first()
    if profile and profile.onboarding_completed:
        return redirect(url_for("web.dashboard"))

    goal = Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first()
    if not goal:
        return redirect(url_for("web.onboarding"))

    four_weeks_ago = _utcnow_naive() - timedelta(weeks=4)
    activity_count = Activity.query.filter(
        Activity.user_id == user.id,
        Activity.date >= four_weeks_ago,
    ).count()

    try:
        race_date = goal.race_date if isinstance(goal.race_date, _date) else _date.fromisoformat(str(goal.race_date))
        days_to_race = (race_date - _date.today()).days
    except Exception:
        days_to_race = None

    return render_template(
        "coach_intro.html",
        user=user,
        goal=goal,
        days_to_race=days_to_race,
        has_strava_data=activity_count > 0,
        strava_activity_count=activity_count,
    )


@web.route("/api/coach-intro/message", methods=["POST"])
@csrf.exempt  # JSON endpoint called by JS fetch — session auth enforces identity
@login_required
def coach_intro_message():
    """Handle one turn of the conversational onboarding chat."""
    user = _current_user()
    data = request.get_json(silent=True) or {}

    conversation   = data.get("conversation", [])
    user_answer    = data.get("user_answer", "")
    current_step   = int(data.get("current_step", 0))
    collected      = dict(data.get("collected", {}))

    goal = Goal.query.filter_by(user_id=user.id).order_by(Goal.id.desc()).first()
    if not goal:
        return jsonify({"error": "no_goal"}), 400

    if current_step > 0 and user_answer and current_step - 1 < len(_STEP_KEYS):
        collected[_STEP_KEYS[current_step - 1]] = user_answer

    if current_step >= 4:
        _save_runner_profile(user.id, collected)
        return jsonify({"done": True, "redirect": url_for("web.dashboard")})

    msg = _get_next_coach_message(current_step, user_answer, collected, goal, user)

    return jsonify({
        "done": False,
        "message": msg.get("text", ""),
        "options": msg.get("options", []),
        "input_type": msg.get("input_type", "options"),
        "next_step": current_step + 1,
        "collected": msg.get("collected", collected),
    })


# ---------------------------------------------------------------------------
# Onboarding & Settings
# ---------------------------------------------------------------------------

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
        pb_5k  = request.form.get("pb_5k",  "").strip()
        pb_10k = request.form.get("pb_10k", "").strip()
        pb_hm  = request.form.get("pb_hm",  "").strip()

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
                    pb_5k=pb_5k,
                    pb_10k=pb_10k,
                    pb_hm=pb_hm,
                )
                return redirect(url_for("web.coach_intro"))
            except Exception:
                error = "Unable to save goal right now. Please try again."

    return render_template("onboarding.html", user=user, goal=get_goal(user.id), error=error)


@web.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    user = _current_user()
    goal = get_goal(user.id)
    profile = RunnerProfile.query.filter_by(user_id=user.id).first()
    error = request.args.get("error")

    if request.method == "POST":
        race_name = request.form.get("race_name", "").strip()
        race_date = request.form.get("race_date", "").strip()
        goal_time = request.form.get("goal_time", "").strip()
        elevation_type = request.form.get("elevation_type", "moderate").strip()
        personal_best = request.form.get("current_pb", "").strip()
        pb_5k  = request.form.get("pb_5k",  "").strip()
        pb_10k = request.form.get("pb_10k", "").strip()
        pb_hm  = request.form.get("pb_hm",  "").strip()
        training_days_per_week = request.form.get("training_days_per_week", "").strip()
        long_run_day = request.form.get("long_run_day", "").strip().lower()
        strength_days_per_week = request.form.get("strength_days_per_week", "").strip()
        preferred_run_time = request.form.get("preferred_run_time", "").strip().lower()

        try:
            race_distance = float(request.form.get("race_distance", "0"))
        except ValueError:
            race_distance = 0

        try:
            training_days_per_week = int(training_days_per_week or 5)
        except ValueError:
            training_days_per_week = 5
        try:
            strength_days_per_week = int(strength_days_per_week or 2)
        except ValueError:
            strength_days_per_week = 2

        training_days_per_week = max(3, min(6, training_days_per_week))
        strength_days_per_week = max(0, min(2, strength_days_per_week))
        if long_run_day not in {"saturday", "sunday", "flexible"}:
            long_run_day = "sunday"
        if preferred_run_time not in {"early_morning", "morning", "evening", "flexible"}:
            preferred_run_time = "flexible"

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
                    pb_5k=pb_5k,
                    pb_10k=pb_10k,
                    pb_hm=pb_hm,
                )
                if not profile:
                    profile = RunnerProfile(user_id=user.id)
                profile.training_days_per_week = training_days_per_week
                profile.long_run_day = long_run_day
                profile.strength_days_per_week = strength_days_per_week
                profile.preferred_run_time = preferred_run_time
                db.session.add(profile)
                db.session.commit()
                return redirect(url_for("web.dashboard"))
            except Exception:
                db.session.rollback()
                error = "Unable to update settings right now. Please try again."

    return render_template("settings.html", user=user, goal=goal, profile=profile, error=error)


# ---------------------------------------------------------------------------
# Strava OAuth
# ---------------------------------------------------------------------------

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
@limiter.limit("10 per hour")
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
