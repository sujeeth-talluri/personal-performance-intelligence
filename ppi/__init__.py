from flask import Flask

from .config import Config
from .extensions import db
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

    db.init_app(app)

    with app.app_context():
        from . import models  # noqa: F401

        db.create_all()

    app.register_blueprint(web)

    return app

