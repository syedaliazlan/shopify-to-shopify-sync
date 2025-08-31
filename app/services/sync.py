# app/services/sync.py
import time
import requests
from typing import Optional

from ..clients.shopify import admin_base, rest_headers, graphql

# -------------------------
# Simple debounce
# -------------------------
_recent = {}
def _debounced(key: str, window_sec: int = 10) -> bool:
    now = time.time()
    last = _recent.get(key, 0)
    _recent[key] = now
    return (now - last) < window_sec

# -------------------------
# Product helpers
# -------------------------
def has_fireplace_tag(prod: dict) -> bool:
    return "fireplace" in (prod.get("tags", "") or "").lower()

def total_inventory(prod: dict) -> int:
    return sum((v or {}).get("inventory_quantity", 0) for v in prod.get("variants", []))

# -------------------------
# GraphQL finders (by SKU)
# -------------------------
FIND_BY_SKU = """
query($q:String!) {
  productVariants(first: 10, query: $q) {
    edges {
      node {
        id
        sku
        product { id title status tags }
      }
    }
  }
}
"""

def find_variant_by_sku(domain: str, token: str, sku: str):
    """Return (product_gid, variant_gid) for this store by SKU, or (None, None)."""
    data = graphql(domain, token, FIND_BY_SKU, {"q": f"sku:{sku}"})
    edges = data.get("data", {}).get("productVariants", {}).get("edges", [])
    if not edges:
        return None, None
    node = edges[0]["node"]
    return node["product"]["id"], node["id"]

def find_product_id_by_any_sku(domain: str, token: str, skus: list[str]) -> Optional[str]:
    """Given a list of SKUs, return first matching product id (numeric) on this store."""
    for sku in skus:
        if not sku:
            continue
        prod_gid, _ = find_variant_by_sku(domain, token, sku)
        if prod_gid:
            return prod_gid.split("/")[-1]
    return None

# -------------------------
# REST helpers
# -------------------------
def get_product(domain: str, token: str, pid: int | str) -> Optional[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("product")
    return None

def put_product(domain: str, token: str, product_payload: dict) -> Optional[dict]:
    r = requests.put(f"{admin_base(domain)}/products/{product_payload['product']['id']}.json",
                     headers=rest_headers(token), json=product_payload, timeout=40)
    if r.status_code in (200, 201):
        return r.json().get("product")
    raise RuntimeError(f"PUT product failed {r.status_code}: {r.text}")

def post_product(domain: str, token: str, product_payload: dict) -> Optional[dict]:
    r = requests.post(f"{admin_base(domain)}/products.json",
                      headers=rest_headers(token), json=product_payload, timeout=40)
    if r.status_code in (200, 201):
        return r.json().get("product")
    raise RuntimeError(f"POST product failed {r.status_code}: {r.text}")

def put_variant(domain: str, token: str, variant_payload: dict) -> Optional[dict]:
    vid = variant_payload["variant"]["id"]
    r = requests.put(f"{admin_base(domain)}/variants/{vid}.json",
                     headers=rest_headers(token), json=variant_payload, timeout=25)
    if r.status_code in (200, 201):
        return r.json().get("variant")
    raise RuntimeError(f"PUT variant failed {r.status_code}: {r.text}")

def post_variant(domain: str, token: str, variant_payload: dict) -> Optional[dict]:
    r = requests.post(f"{admin_base(domain)}/variants.json",
                      headers=rest_headers(token), json=variant_payload, timeout=25)
    if r.status_code in (200, 201):
        return r.json().get("variant")
    raise RuntimeError(f"POST variant failed {r.status_code}: {r.text}")

def delete_product(domain: str, token: str, pid: str):
    requests.delete(f"{admin_base(domain)}/products/{pid}.json",
                    headers=rest_headers(token), timeout=25)

# -------------------------
# Inventory helpers
# -------------------------
def _get_inventory_item_id(domain: str, token: str, variant_id: str | int) -> Optional[int]:
    r = requests.get(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        return None
    return (r.json().get("variant") or {}).get("inventory_item_id")

def _get_available_at_location(domain: str, token: str, inventory_item_id: int, location_id_legacy: str) -> Optional[int]:
    params = {
        "inventory_item_ids": inventory_item_id,
        "location_ids": location_id_legacy,
        "limit": 1
    }
    r = requests.get(f"{admin_base(domain)}/inventory_levels.json",
                     headers=rest_headers(token), params=params, timeout=20)
    if r.status_code != 200:
        return None
    levels = r.json().get("inventory_levels", [])
    if not levels:
        return None
    return levels[0].get("available")

def _connect_level_if_needed(domain: str, token: str, inventory_item_id: int, location_id_legacy: str):
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id_legacy)}
    # 422 if already connected is fine.
    requests.post(f"{admin_base(domain)}/inventory_levels/connect.json",
                  headers=rest_headers(token), json=payload, timeout=20)

def set_absolute_inventory_by_sku(domain: str, token: str, sku: str, qty: int, location_id_legacy: str):
    """Force available quantity at a location for variant identified by SKU."""
    if qty is None:
        return
    prod_gid, var_gid = find_variant_by_sku(domain, token, sku)
    if not var_gid:
        return
    variant_id = var_gid.split("/")[-1]
    inventory_item_id = _get_inventory_item_id(domain, token, variant_id)
    if not inventory_item_id:
        return
    _connect_level_if_needed(domain, token, inventory_item_id, location_id_legacy)
    payload = {
        "location_id": int(location_id_legacy),
        "inventory_item_id": int(inventory_item_id),
        "available": int(qty)
    }
    r = requests.post(f"{admin_base(domain)}/inventory_levels/set.json",
                      headers=rest_headers(token), json=payload, timeout=20)
    if r.status_code not in (200, 201):
        print(f"[inventory] set failed for {sku} on {domain}: {r.status_code} {r.text}")

def adjust_inventory_by_sku(domain: str, token: str, sku: str, delta: int, location_id_legacy: str):
    """Increase or decrease quantity for a variant identified by SKU at a given location."""
    if not sku or not delta:
        return
    prod_gid, var_gid = find_variant_by_sku(domain, token, sku)
    if not var_gid:
        print(f"[inventory] No variant for SKU {sku} on {domain}")
        return
    variant_id = var_gid.split("/")[-1]
    r = requests.get(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        print(f"[inventory] Could not read variant {variant_id} on {domain}: {r.text}")
        return
    inventory_item_id = (r.json().get("variant") or {}).get("inventory_item_id")
    if not inventory_item_id:
        print(f"[inventory] No inventory_item_id for SKU {sku} on {domain}")
        return
    adj = {
        "location_id": int(location_id_legacy),
        "inventory_item_id": int(inventory_item_id),
        "available_adjustment": int(delta)
    }
    rr = requests.post(f"{admin_base(domain)}/inventory_levels/adjust.json",
                       headers=rest_headers(token), json=adj, timeout=20)
    if rr.status_code not in (200, 201):
        print(f"[inventory] Adjust failed for {sku} on {domain}: {rr.status_code} {rr.text}")

def mirror_inventory_values_from_src_to_dst(src_store: dict, dst_store: dict, src_prod: dict, src_location_legacy: str, dst_location_legacy: str):
    """For each SKU in src product, read available at source location and set the same on destination."""
    for v in src_prod.get("variants", []):
        sku = v.get("sku")
        if not sku:
            continue
        # get source inventory level
        prod_gid, var_gid = find_variant_by_sku(src_store["domain"], src_store["token"], sku)
        if not var_gid:
            continue
        src_variant_id = var_gid.split("/")[-1]
        inv_item_id = _get_inventory_item_id(src_store["domain"], src_store["token"], src_variant_id)
        if not inv_item_id:
            continue
        available = _get_available_at_location(src_store["domain"], src_store["token"], inv_item_id, src_location_legacy)
        if available is None:
            available = 0
        # set exact value on destination
        set_absolute_inventory_by_sku(dst_store["domain"], dst_store["token"], sku, available, dst_location_legacy)

# -------------------------
# Core: Upsert / Delete by SKU
# -------------------------
def upsert_product_by_sku(src_store: dict, dst_store: dict, src_prod: dict):
    """
    Create or update the product on dst_store using SKU mapping.
    - Only runs if 'fireplace' tag is present.
    - Deletion/unpublish is enforced only when source is TAF.
    - After create/update, inventory is mirrored from source to destination at configured locations.
    """
    key = f"{src_store['name']}:{src_prod['id']}"
    if _debounced(key):
        return

    # Out of scope products should be removed on destination if they exist
    if not has_fireplace_tag(src_prod):
        _delete_dst_if_exists_by_any_sku(dst_store, src_prod)
        return

    # Availability rule is authoritative from TAF only
    if src_store["name"] == "TAF":
        if (src_prod.get("status") in ("draft", "archived")) or (total_inventory(src_prod) <= 0):
            _delete_dst_if_exists_by_any_sku(dst_store, src_prod)
            return

    # Does the destination already have this product (by any SKU)
    src_skus = [v.get("sku") for v in src_prod.get("variants", []) if v.get("sku")]
    dst_pid = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], src_skus)

    # Build base payload
    product_payload = {
        "product": {
            "title": src_prod.get("title"),
            "body_html": src_prod.get("body_html", ""),
            "vendor": src_prod.get("vendor"),
            "product_type": src_prod.get("product_type"),
            "tags": src_prod.get("tags", ""),
            "status": src_prod.get("status", "active"),
            "options": [{"name": o["name"]} for o in (src_prod.get("options") or [])],
            "images": [{"src": i.get("src")} for i in (src_prod.get("images") or [])],
        }
    }

    if dst_pid:
        # Update product level info
        product_payload["product"]["id"] = dst_pid
        put_product(dst_store["domain"], dst_store["token"], product_payload)

        # Update or create variants by SKU
        for v in src_prod.get("variants", []):
            sku = v.get("sku")
            if not sku:
                continue
            _, var_gid = find_variant_by_sku(dst_store["domain"], dst_store["token"], sku)
            if not var_gid:
                # Create the missing variant
                new_payload = {
                    "variant": {
                        "product_id": int(dst_pid),
                        "sku": v.get("sku"),
                        "price": v.get("price"),
                        "compare_at_price": v.get("compare_at_price"),
                        "option1": v.get("option1"),
                        "option2": v.get("option2"),
                        "option3": v.get("option3"),
                        "requires_shipping": v.get("requires_shipping", True),
                        "taxable": v.get("taxable", True),
                        "barcode": v.get("barcode"),
                        "inventory_management": "shopify"
                    }
                }
                try:
                    post_variant(dst_store["domain"], dst_store["token"], new_payload)
                except Exception as e:
                    print(f"[upsert] Create variant failed for SKU {sku} on {dst_store['domain']}: {e}")
                continue

            dst_var_id = var_gid.split("/")[-1]
            try:
                put_variant(dst_store["domain"], dst_store["token"], {
                    "variant": {
                        "id": int(dst_var_id),
                        "price": v.get("price"),
                        "compare_at_price": v.get("compare_at_price"),
                    }
                })
            except Exception as e:
                print(f"[upsert] Update variant failed for SKU {sku} on {dst_store['domain']}: {e}")

        # Mirror inventory values from source to destination
        mirror_inventory_values_from_src_to_dst(
            src_store, dst_store, src_prod,
            src_store["location_id"], dst_store["location_id"]
        )
    else:
        # Create new product with all variants
        product_payload["product"]["variants"] = [{
            "sku": v.get("sku"),
            "price": v.get("price"),
            "compare_at_price": v.get("compare_at_price"),
            "taxable": v.get("taxable", True),
            "requires_shipping": v.get("requires_shipping", True),
            "barcode": v.get("barcode"),
            "option1": v.get("option1"),
            "option2": v.get("option2"),
            "option3": v.get("option3"),
            "inventory_management": "shopify"
        } for v in (src_prod.get("variants") or [])]

        try:
            post_product(dst_store["domain"], dst_store["token"], product_payload)
        except Exception as e:
            print(f"[upsert] Create product failed on {dst_store['domain']}: {e}")
            return

        # Mirror inventory values from source to destination
        mirror_inventory_values_from_src_to_dst(
            src_store, dst_store, src_prod,
            src_store["location_id"], dst_store["location_id"]
        )

def _delete_dst_if_exists_by_any_sku(dst_store: dict, src_prod: dict):
    skus = [v.get("sku") for v in src_prod.get("variants", []) if v.get("sku")]
    if not skus:
        return
    dst_pid = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], skus)
    if dst_pid:
        delete_product(dst_store["domain"], dst_store["token"], dst_pid)

# -------------------------
# Order-driven inventory sync
# -------------------------
def mirror_sale_to_other_store(other_store: dict, sku: str, qty: int, other_location_legacy_id: str):
    """Decrease inventory on other_store for this SKU by qty (sale)."""
    if not sku or not qty:
        return
    adjust_inventory_by_sku(other_store["domain"], other_store["token"], sku, -abs(qty), other_location_legacy_id)

def mirror_cancel_to_other_store(other_store: dict, sku: str, qty: int, other_location_legacy_id: str):
    """Increase inventory on other_store for this SKU by qty (restock)."""
    if not sku or not qty:
        return
    adjust_inventory_by_sku(other_store["domain"], other_store["token"], sku, +abs(qty), other_location_legacy_id)
