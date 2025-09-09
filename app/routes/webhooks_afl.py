# app/routes/webhooks_afl.py
import json
import threading
from flask import Blueprint

from ..config import AFL, TAF
from ..utils.security import verify_webhook_hmac
from ..services.sync import (
    handle_product_event,
    mirror_sale_to_other_store,
    mirror_cancel_to_other_store,
)

bp = Blueprint("webhooks_afl", __name__)

@bp.post("/products")
def products():
    raw = verify_webhook_hmac(AFL["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    pid = payload.get("id")
    print(f"[AFL] /products webhook received. PID={pid}")
    if not pid:
        return "OK", 200

    def worker():
        try:
            print(f"[AFL ➝ TAF] handle_product_event start. PID={pid}")
            handle_product_event(AFL, TAF, payload)
            print(f"[AFL ➝ TAF] handle_product_event done. PID={pid}")
        except Exception as e:
            print(f"[ERROR][AFL ➝ TAF] products worker PID={pid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202


@bp.post("/inventory")
def inventory():
    verify_webhook_hmac(AFL["secret"])
    print("[AFL] /inventory webhook received.")
    # Minimal by design; orders drive cross-store stock adjustments
    return "Accepted", 202


@bp.post("/orders_paid")
def orders_paid():
    raw = verify_webhook_hmac(AFL["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    print(f"[AFL] /orders_paid webhook received. OID={oid}")

    def worker():
        try:
            for li in order.get("line_items", []):
                sku = li.get("sku")
                qty = li.get("quantity", 0)
                if sku and qty:
                    print(f"[AFL ➝ TAF][orders] OID={oid} SKU={sku} -{qty}")
                    mirror_sale_to_other_store(TAF, sku, qty, TAF.get("location_id"))
        except Exception as e:
            print(f"[ERROR][AFL ➝ TAF] orders_paid worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202


@bp.post("/orders_cancelled")
def orders_cancelled():
    raw = verify_webhook_hmac(AFL["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    print(f"[AFL] /orders_cancelled webhook received. OID={oid}")

    def worker():
        try:
            for li in order.get("line_items", []):
                sku = li.get("sku")
                qty = li.get("quantity", 0)
                if sku and qty:
                    print(f"[AFL ➝ TAF][orders] OID={oid} SKU={sku} +{qty}")
                    mirror_cancel_to_other_store(TAF, sku, qty, TAF.get("location_id"))
        except Exception as e:
            print(f"[ERROR][AFL ➝ TAF] orders_cancelled worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202
