import os
from flask import Flask
from dotenv import load_dotenv

def create_app():
    load_dotenv()
    app = Flask(__name__)

    # Blueprints
    from .routes.register import bp as register_bp
    from .routes.webhooks_taf import bp as taf_bp
    from .routes.webhooks_afl import bp as afl_bp

    app.register_blueprint(register_bp, url_prefix="/register_webhooks")
    app.register_blueprint(taf_bp, url_prefix="/taf/webhooks")
    app.register_blueprint(afl_bp, url_prefix="/afl/webhooks")

    @app.get("/health")
    def health():
        return {"ok": True}, 200

    return app
