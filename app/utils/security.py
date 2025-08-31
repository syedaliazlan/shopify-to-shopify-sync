import base64, hashlib, hmac
from flask import request, abort

def verify_webhook_hmac(secret: str):
    raw = request.get_data()
    their_hmac = request.headers.get("X-Shopify-Hmac-Sha256", "")
    digest = hmac.new(secret.encode(), raw, hashlib.sha256).digest()
    if not hmac.compare_digest(base64.b64encode(digest).decode(), their_hmac):
        abort(401)
    return raw
