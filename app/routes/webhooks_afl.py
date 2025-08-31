# app/routes/webhooks_afl.py
import json
import threading
from flask import Blueprint

from ..config import AFL, TAF
from ..utils.security import verify_webhook_hmac
from ..services.sync import (
    get_product,
    upsert_product_by_sku,
    mirror_sale_to_other_store,
    mirror_cancel_to_other_store,
)

bp = Blueprint("webhooks_afl", __name__)

@bp.post("/products")
def products():
    """Product create/update/delete events from AFL."""
    raw = verify_webhook_hmac(AFL["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    pid = payload.get("id")
    if not pid:
        return "OK", 200

    def worker():
        try:
            prod = get_product(AFL["domain"], AFL["token"], pid)
            if not prod:
                print(f"[AFL] product {pid} not found on fetch")
                return
            upsert_product_by_sku(AFL, TAF, prod)
        except Exception as e:
            print(f"[AFL] products worker error: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202


@bp.post("/inventory")
def inventory():
    """
    Inventory level updates on AFL.
    Optional: mirror every change back to TAF by parsing payload and calling a
    set/adjust helper. For now we rely on orders webhooks to keep quantities aligned.
    """
    verify_webhook_hmac(AFL["secret"])
    return "Accepted", 202


@bp.post("/orders_paid")
def orders_paid():
    """A sale happened on AFL → decrease TAF inventory for each SKU/qty."""
    raw = verify_webhook_hmac(AFL["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    try:
        for li in order.get("line_items", []):
            sku = li.get("sku")
            qty = li.get("quantity", 0)
            if sku and qty:
                threading.Thread(
                    target=mirror_sale_to_other_store,
                    args=(TAF, sku, qty, TAF["location_id"]),
                    daemon=True,
                ).start()
    except Exception as e:
        print(f"[AFL] orders_paid worker error: {e}")
    return "Accepted", 202


@bp.post("/orders_cancelled")
def orders_cancelled():
    """An order was cancelled on AFL → restock TAF inventory for each SKU/qty."""
    raw = verify_webhook_hmac(AFL["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    try:
        for li in order.get("line_items", []):
            sku = li.get("sku")
            qty = li.get("quantity", 0)
            if sku and qty:
                threading.Thread(
                    target=mirror_cancel_to_other_store,
                    args=(TAF, sku, qty, TAF["location_id"]),
                    daemon=True,
                ).start()
    except Exception as e:
        print(f"[AFL] orders_cancelled worker error: {e}")
    return "Accepted", 202
