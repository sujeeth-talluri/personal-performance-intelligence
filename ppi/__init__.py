from flask import Flask

from .config import Config
from .db import initialize_database, close_db
from .routes import web


def create_app(config_object=Config):
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    app.config.from_object(config_object)

    initialize_database(app.config["DATABASE_PATH"])

    app.teardown_appcontext(close_db)
    app.register_blueprint(web)

    return app
