# app/routes/register.py
from flask import Blueprint
import requests
from ..config import BASE_URL, TAF, AFL
from ..clients.shopify import admin_base, rest_headers

bp = Blueprint("register", __name__)

def _ensure(domain: str, token: str, subs: list[tuple[str, str]]):
    if not BASE_URL:
        return "Missing BASE_URL in env.", 500
    if not (domain and token):
        return f"Missing domain/token for {domain or 'unknown'}", 500

    base = admin_base(domain)
    headers = rest_headers(token)

    # Read existing webhooks
    try:
        resp = requests.get(f"{base}/webhooks.json", headers=headers, timeout=20)
        resp.raise_for_status()
        existing = resp.json().get("webhooks", [])
    except Exception as e:
        return f"Failed to read existing webhooks: {e}", 500

    out = []
    for topic, address in subs:
        # Find any existing webhook for this topic
        found = [w for w in existing if w.get("topic") == topic]

        if found:
            # If any already points to the desired address -> OK
            if any(w.get("address") == address for w in found):
                out.append(f"OK {topic}")
                continue

            # Otherwise update the first one to the new address (prevents duplicates when your URL changes)
            wid = found[0].get("id")
            try:
                upd = requests.put(
                    f"{base}/webhooks/{wid}.json",
                    headers=headers,
                    json={"webhook": {"id": wid, "address": address, "format": "json"}},
                    timeout=20,
                )
                if upd.status_code in (200, 201):
                    out.append(f"UPDATED {topic}")
                else:
                    out.append(f"FAIL {topic} {upd.status_code} {upd.text}")
            except Exception as e:
                out.append(f"FAIL {topic} exception {e}")
            continue

        # No existing webhook for this topic -> create
        try:
            crt = requests.post(
                f"{base}/webhooks.json",
                headers=headers,
                json={"webhook": {"topic": topic, "address": address, "format": "json"}},
                timeout=20,
            )
            if crt.status_code in (201, 202):
                out.append(f"CREATED {topic}")
            else:
                out.append(f"FAIL {topic} {crt.status_code} {crt.text}")
        except Exception as e:
            out.append(f"FAIL {topic} exception {e}")

    return "; ".join(out), 200

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
