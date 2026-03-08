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

    db.init_app(app)

    with app.app_context():
        from . import models  # noqa: F401

        db.create_all()

    app.register_blueprint(web)

    return app
