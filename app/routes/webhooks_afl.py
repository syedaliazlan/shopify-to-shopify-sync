# app/routes/webhooks_afl.py
import json
import threading
import time
from flask import Blueprint, request

from ..config import AFL, TAF
from ..utils.security import verify_webhook_hmac
from ..utils.logger import info, warn, error
from ..services.sync import (
    handle_product_event,
    mirror_sale_to_other_store,
    mirror_cancel_to_other_store,
)

bp = Blueprint("webhooks_afl", __name__)

# In-memory idempotency (best-effort)
_SEEN_IDS: dict[str, float] = {}
_SEEN_TTL = 60 * 10  # 10 minutes

def _seen(webhook_id: str) -> bool:
    now = time.time()
    # GC old ids
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
    """
    Return list of (sku, qty_to_add_back) derived from refunds that restock inventory.
    Falls back to line_items only if Shopify doesn't provide refunds payloads.
    """
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
            # If Shopify flags restock for this refund line, count it
            if (ri.get("restock_type") or "").lower() in ("cancel", "return"):
                had_restock = True
                deltas[sku] = deltas.get(sku, 0) + qty
    if had_restock:
        return list(deltas.items())

    # Fallback: if no refunds/restock info, add back whole line_items only if order['restock'] is true
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

    raw = verify_webhook_hmac(AFL["secret"])
    payload = json.loads(raw.decode("utf-8")) if raw else {}
    pid = payload.get("id")
    info(f"[AFL] /products webhook received. PID={pid}")

    # Respond 200 immediately; do work async
    def worker():
        try:
            info(f"[AFL ➝ TAF] handle_product_event start. PID={pid}")
            handle_product_event(AFL, TAF, payload)
            info(f"[AFL ➝ TAF] handle_product_event done. PID={pid}")
        except Exception as e:
            error(f"[AFL ➝ TAF] products worker PID={pid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200


@bp.post("/inventory")
def inventory():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    verify_webhook_hmac(AFL["secret"])
    info("[AFL] /inventory webhook received.")
    # Minimal by design; orders webhooks drive cross-store stock adjustments
    return "OK", 200


@bp.post("/orders_paid")
def orders_paid():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    raw = verify_webhook_hmac(AFL["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    info(f"[AFL] /orders_paid webhook received. OID={oid}")

    def worker():
        try:
            for li in (order.get("line_items") or []):
                sku = (li.get("sku") or "").strip()
                qty = int(li.get("quantity") or 0)
                if not sku or qty <= 0:
                    continue
                info(f"[AFL ➝ TAF][orders] OID={oid} SKU={sku} -{qty}")
                mirror_sale_to_other_store(TAF, sku, qty, TAF.get("location_id"))
        except Exception as e:
            error(f"[AFL ➝ TAF] orders_paid worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200


@bp.post("/orders_cancelled")
def orders_cancelled():
    webhook_id = request.headers.get("X-Shopify-Webhook-Id", "")
    if _seen(webhook_id):
        return "OK", 200

    raw = verify_webhook_hmac(AFL["secret"])
    order = json.loads(raw.decode("utf-8")) if raw else {}
    oid = order.get("id")
    info(f"[AFL] /orders_cancelled webhook received. OID={oid}")

    def worker():
        try:
            for sku, qty in _restock_deltas_from_cancelled(order):
                info(f"[AFL ➝ TAF][orders] OID={oid} SKU={sku} +{qty}")
                mirror_cancel_to_other_store(TAF, sku, qty, TAF.get("location_id"))
        except Exception as e:
            error(f"[AFL ➝ TAF] orders_cancelled worker OID={oid}: {e}")

    threading.Thread(target=worker, daemon=True).start()
    return "OK", 200
