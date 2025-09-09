import os
import sys
import logging
from flask import Flask
from dotenv import load_dotenv


def create_app():
    load_dotenv()
    app = Flask(__name__)

    # =========================================================
    # Configure logging so logs show up on Render
    # =========================================================
    gunicorn_error = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_error.handlers
    app.logger.setLevel(logging.INFO)

    # Also add a stdout handler (for safety)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s", "%H:%M:%S"))
    app.logger.addHandler(sh)

    # =========================================================
    # Blueprints
    # =========================================================
    from .routes.register import bp as register_bp
    from .routes.webhooks_taf import bp as taf_bp
    from .routes.webhooks_afl import bp as afl_bp
    from .routes.setup_metafields import bp as setup_bp

    app.register_blueprint(register_bp, url_prefix="/register_webhooks")
    app.register_blueprint(taf_bp, url_prefix="/taf/webhooks")
    app.register_blueprint(afl_bp, url_prefix="/afl/webhooks")
    app.register_blueprint(setup_bp, url_prefix="/setup/metafields")

    # =========================================================
    # Health check
    # =========================================================
    @app.get("/health")
    def health():
        app.logger.info("Health check endpoint called")
        return {"ok": True}, 200

    return app
