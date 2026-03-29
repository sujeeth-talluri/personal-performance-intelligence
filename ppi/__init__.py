from flask import Flask

from .config import Config
from .extensions import csrf, db, limiter
from .routes import web


def create_app(config_object=Config):
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    app.config.from_object(config_object)
    secret = app.config.get("SECRET_KEY")
    if (not app.config.get("TESTING")) and (not secret or secret == "dev-secret-change-me"):
        raise RuntimeError("SECRET_KEY is required for production.")

    if not app.config.get("SQLALCHEMY_DATABASE_URI"):
        raise RuntimeError("DATABASE_URL is required. Configure a persistent PostgreSQL connection.")

    # Warn loudly at startup if AI keys are missing — prevents silent
    # failures when the AI coach engine is first called at request time.
    if not app.config.get("TESTING"):
        if not app.config.get("ANTHROPIC_API_KEY"):
            app.logger.warning(
                "ANTHROPIC_API_KEY is not set — AI coaching plan generation will "
                "fall back to the rule-based engine. Set the key in your environment "
                "to enable Claude-powered coaching."
            )
        if not app.config.get("OPENAI_API_KEY"):
            app.logger.warning(
                "OPENAI_API_KEY is not set — any OpenAI-dependent features will "
                "be unavailable. Set the key if you intend to use OpenAI models."
            )

    db.init_app(app)
    csrf.init_app(app)
    limiter.init_app(app)

    with app.app_context():
        from . import models  # noqa: F401

    if app.config.get("TESTING"):
        with app.app_context():
            db.create_all()

    from . import routes_auth  # noqa: F401  — registers auth/onboarding/strava routes on `web`
    app.register_blueprint(web)

    return app

