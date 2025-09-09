# app/routes/webhooks_taf.py
import json
import threading
from flask import Blueprint

from ..config import TAF, AFL
from ..utils.security import verify_webhook_hmac
from ..services.sync import (
    handle_product_event,
    mirror_sale_to_other_store,
    mirror_cancel_to_other_store,
)

bp = Blueprint("webhooks_taf", __name__)

@bp.post("/products")
def products():
    raw = verify_webhook_hmac(TAF["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    pid = payload.get("id")
    print(f"[TAF] /products webhook received. PID={pid}")
    if not pid:
        return "OK", 200

    def worker():
        try:
            print(f"[TAF ➝ AFL] handle_product_event start. PID={pid}")
            handle_product_event(TAF, AFL, payload)
            print(f"[TAF ➝ AFL] handle_product_event done. PID={pid}")
        except Exception as e:
            print(f"[ERROR][TAF ➝ AFL] products worker PID={pid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202


@bp.post("/inventory")
def inventory():
    verify_webhook_hmac(TAF["secret"])
    print("[TAF] /inventory webhook received.")
    # We rely primarily on orders webhooks; inventory handler kept minimal
    return "Accepted", 202


@bp.post("/orders_paid")
def orders_paid():
    raw = verify_webhook_hmac(TAF["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    print(f"[TAF] /orders_paid webhook received. OID={oid}")

    def worker():
        try:
            for li in order.get("line_items", []):
                sku = li.get("sku")
                qty = li.get("quantity", 0)
                if sku and qty:
                    print(f"[TAF ➝ AFL][orders] OID={oid} SKU={sku} -{qty}")
                    mirror_sale_to_other_store(AFL, sku, qty, AFL.get("location_id"))
        except Exception as e:
            print(f"[ERROR][TAF ➝ AFL] orders_paid worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202


@bp.post("/orders_cancelled")
def orders_cancelled():
    raw = verify_webhook_hmac(TAF["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    print(f"[TAF] /orders_cancelled webhook received. OID={oid}")

    def worker():
        try:
            for li in order.get("line_items", []):
                sku = li.get("sku")
                qty = li.get("quantity", 0)
                if sku and qty:
                    print(f"[TAF ➝ AFL][orders] OID={oid} SKU={sku} +{qty}")
                    mirror_cancel_to_other_store(AFL, sku, qty, AFL.get("location_id"))
        except Exception as e:
            print(f"[ERROR][TAF ➝ AFL] orders_cancelled worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202
