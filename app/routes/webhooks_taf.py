# app/routes/webhooks_taf.py
import json
import threading
from flask import Blueprint

from ..config import TAF, AFL
from ..utils.security import verify_webhook_hmac
from ..services.sync import (
    get_product,
    upsert_product_by_sku,
    mirror_sale_to_other_store,
    mirror_cancel_to_other_store,
)

bp = Blueprint("webhooks_taf", __name__)

@bp.post("/products")
def products():
    """Product create/update/delete events from TAF."""
    raw = verify_webhook_hmac(TAF["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    pid = payload.get("id")
    if not pid:
        return "OK", 200

    def worker():
        try:
            prod = get_product(TAF["domain"], TAF["token"], pid)
            if not prod:
                print(f"[TAF] product {pid} not found on fetch")
                return
            upsert_product_by_sku(TAF, AFL, prod)
        except Exception as e:
            print(f"[TAF] products worker error: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "Accepted", 202


@bp.post("/inventory")
def inventory():
    """
    Inventory level updates on TAF.
    Optional: if you want to mirror *every* inventory_levels/update, you can
    expand this to parse the payload and set inventory on AFL by SKU.
    """
    verify_webhook_hmac(TAF["secret"])
    # Intentionally minimal; sales/cancellations are handled by orders webhooks
    return "Accepted", 202


@bp.post("/orders_paid")
def orders_paid():
    """A sale happened on TAF → decrease AFL inventory for each SKU/qty."""
    raw = verify_webhook_hmac(TAF["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    try:
        for li in order.get("line_items", []):
            sku = li.get("sku")
            qty = li.get("quantity", 0)
            if sku and qty:
                threading.Thread(
                    target=mirror_sale_to_other_store,
                    args=(AFL, sku, qty, AFL["location_id"]),
                    daemon=True,
                ).start()
    except Exception as e:
        print(f"[TAF] orders_paid worker error: {e}")
    return "Accepted", 202


@bp.post("/orders_cancelled")
def orders_cancelled():
    """An order was cancelled on TAF → restock AFL inventory for each SKU/qty."""
    raw = verify_webhook_hmac(TAF["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    try:
        for li in order.get("line_items", []):
            sku = li.get("sku")
            qty = li.get("quantity", 0)
            if sku and qty:
                threading.Thread(
                    target=mirror_cancel_to_other_store,
                    args=(AFL, sku, qty, AFL["location_id"]),
                    daemon=True,
                ).start()
    except Exception as e:
        print(f"[TAF] orders_cancelled worker error: {e}")
    return "Accepted", 202
