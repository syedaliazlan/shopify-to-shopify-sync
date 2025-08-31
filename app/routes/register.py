from flask import Blueprint
import requests
from ..config import BASE_URL, TAF, AFL, API_VERSION
from ..clients.shopify import admin_base, rest_headers

bp = Blueprint("register", __name__)

def _ensure(domain, token, subs):
    base = admin_base(domain); headers = rest_headers(token)
    existing = requests.get(f"{base}/webhooks.json", headers=headers, timeout=20).json().get("webhooks", [])
    out = []
    for topic, address in subs:
        if any(w.get("topic")==topic and w.get("address")==address for w in existing):
            out.append(f"OK {topic}"); continue
        r = requests.post(f"{base}/webhooks.json", headers=headers, json={"webhook":{"topic":topic,"address":address,"format":"json"}}, timeout=20)
        out.append("CREATED " + topic if r.status_code in (201,202) else f"FAIL {topic} {r.status_code}")
    return "; ".join(out)

@bp.get("/taf")
def reg_taf():
    subs = [
        ("products/create", f"{BASE_URL}/taf/webhooks/products"),
        ("products/update", f"{BASE_URL}/taf/webhooks/products"),
        ("products/delete", f"{BASE_URL}/taf/webhooks/products"),
        ("inventory_levels/update", f"{BASE_URL}/taf/webhooks/inventory"),
        ("orders/paid", f"{BASE_URL}/taf/webhooks/orders_paid"),
        ("orders/cancelled", f"{BASE_URL}/taf/webhooks/orders_cancelled"),
    ]
    return _ensure(TAF["domain"], TAF["token"], subs)

@bp.get("/afl")
def reg_afl():
    subs = [
        ("products/create", f"{BASE_URL}/afl/webhooks/products"),
        ("products/update", f"{BASE_URL}/afl/webhooks/products"),
        ("products/delete", f"{BASE_URL}/afl/webhooks/products"),
        ("inventory_levels/update", f"{BASE_URL}/afl/webhooks/inventory"),
        ("orders/paid", f"{BASE_URL}/afl/webhooks/orders_paid"),
        ("orders/cancelled", f"{BASE_URL}/afl/webhooks/orders_cancelled"),
    ]
    return _ensure(AFL["domain"], AFL["token"], subs)
