# app/routes/webhooks_taf.py
import json
import threading
import time
from flask import Blueprint, request

from ..config import TAF, AFL
from ..utils.security import verify_webhook_hmac
from ..utils.logger import info, warn, error
from ..services.sync import (
    handle_product_event,
    mirror_sale_to_other_store,
    mirror_cancel_to_other_store,
    handle_inventory_level_update,
)

bp = Blueprint("webhooks_taf", __name__)

# In-memory idempotency (best-effort)
_SEEN_IDS: dict[str, float] = {}
_SEEN_TTL = 60 * 10  # 10 minutes

def _seen(webhook_id: str) -> bool:
    now = time.time()
    for k, ts in list(_SEEN_IDS.items()):
        if now - ts > _SEEN_TTL:
            _SEEN_IDS.pop(k, None)
    if not webhook_id:
        return False
    if webhook_id in _SEEN_IDS:
        return True
    _SEEN_IDS[webhook_id] = now
    return False

def _restock_deltas_from_cancelled(order: dict) -> list[tuple[str, int]]:
    deltas: dict[str, int] = {}
    refunds = order.get("refunds") or []
    had_restock = False
    for refund in refunds:
        for ri in (refund.get("refund_line_items") or []):
            li = ri.get("line_item") or {}
            sku = (li.get("sku") or "").strip()
            qty = int(ri.get("quantity") or 0)
            if not sku or qty <= 0:
                continue
            if (ri.get("restock_type") or "").lower() in ("cancel", "return"):
                had_restock = True
                deltas[sku] = deltas.get(sku, 0) + qty
    if had_restock:
        return list(deltas.items())
    if order.get("restock") is True:
        for li in (order.get("line_items") or []):
            sku = (li.get("sku") or "").strip()
            qty = int(li.get("quantity") or 0)
            if not sku or qty <= 0:
                continue
            deltas[sku] = deltas.get(sku, 0) + qty
    return list(deltas.items())


@bp.post("/products")
def products():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    raw = verify_webhook_hmac(TAF["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    pid = payload.get("id")
    info(f"[TAF] /products webhook received. PID={pid}")

    def worker():
        try:
            info(f"[TAF ➝ AFL] handle_product_event start. PID={pid}")
            handle_product_event(TAF, AFL, payload)
            info(f"[TAF ➝ AFL] handle_product_event done. PID={pid}")
        except Exception as e:
            error(f"[TAF ➝ AFL] products worker PID={pid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200


@bp.post("/inventory")
def inventory():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    raw = verify_webhook_hmac(TAF["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    inv_item_id = payload.get("inventory_item_id")
    info(f"[TAF] /inventory webhook received. inventory_item_id={inv_item_id}")

    def worker():
        try:
            handle_inventory_level_update(TAF, AFL, payload)
        except Exception as e:
            error(f"[TAF ➝ AFL] inventory worker inv_item_id={inv_item_id}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200


@bp.post("/orders_paid")
def orders_paid():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    raw = verify_webhook_hmac(TAF["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    info(f"[TAF] /orders_paid webhook received. OID={oid}")

    def worker():
        try:
            for li in (order.get("line_items") or []):
                sku = (li.get("sku") or "").strip()
                qty = int(li.get("quantity") or 0)
                if not sku or qty <= 0:
                    continue
                info(f"[TAF ➝ AFL][orders] OID={oid} SKU={sku} -{qty}")
                mirror_sale_to_other_store(AFL, sku, qty, AFL.get("location_id"))
        except Exception as e:
            error(f"[TAF ➝ AFL] orders_paid worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200


@bp.post("/orders_cancelled")
def orders_cancelled():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    raw = verify_webhook_hmac(TAF["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    info(f"[TAF] /orders_cancelled webhook received. OID={oid}")

    def worker():
        try:
            for sku, qty in _restock_deltas_from_cancelled(order):
                info(f"[TAF ➝ AFL][orders] OID={oid} SKU={sku} +{qty}")
                mirror_cancel_to_other_store(AFL, sku, qty, AFL.get("location_id"))
        except Exception as e:
            error(f"[TAF ➝ AFL] orders_cancelled worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200
