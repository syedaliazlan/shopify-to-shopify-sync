# app/services/sync.py
import json
import os
import time
import threading
from typing import Optional, Tuple, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..clients.shopify import admin_base, rest_headers, graphql
from ..utils.logger import debug, info, warn, error

# =========================================================
# Tracing (toggle in Render: SYNC_TRACE=1)
# =========================================================
TRACE = os.getenv("SYNC_TRACE", "0") == "1"
def decide(msg: str):
    if TRACE:
        info(f"[decide] {msg}")

# =========================================================
# Global toggles
# =========================================================
# Disable video sync by default (Shopify cross-store hosted video move is not practical)
DISABLE_VIDEOS = os.getenv("SYNC_DISABLE_VIDEOS", "1") == "1"

# "Hold" edit tag behaviour while working on TAF product
HOLD_BEHAVIOUR = os.getenv("SYNC_HOLD_BEHAVIOUR", "skip").lower()  # "skip" | "draft"
HOLD_TAGS = {"nosync", "no-sync", "do-not-sync", "sync:hold"}

def has_hold_tag(prod: dict) -> bool:
    tags = (prod.get("tags") or "").lower()
    return any(t in tags for t in HOLD_TAGS)

# =========================================================
# Locks, Debounce & Mute
# =========================================================
_locks: dict[str, threading.Lock] = {}
_RECENT: dict[str, float] = {}        # per (store:pid)
_MUTE_UNTIL: dict[str, float] = {}    # per (store:pid)

DEBOUNCE_SEC = 25
MUTE_SEC = 25

def _key(store_name: str, pid: str | int) -> str:
    return f"{store_name}:{pid}"

def _lock_for(store_name: str, pid: str | int) -> threading.Lock:
    k = _key(store_name, pid)
    if k not in _locks:
        _locks[k] = threading.Lock()
    return _locks[k]

def _debounced(k: str, window_sec: int = DEBOUNCE_SEC) -> bool:
    now = time.time()
    last = _RECENT.get(k, 0)
    _RECENT[k] = now
    return (now - last) < window_sec

def _muted(k: str) -> bool:
    return time.time() < _MUTE_UNTIL.get(k, 0)

def _mute_for(k: str, seconds: int = MUTE_SEC):
    _MUTE_UNTIL[k] = time.time() + seconds

# =========================================================
# Utilities & hashing
# =========================================================
ORIGIN_NS = "sync"
ORIGIN_KEY = "origin"

def has_fireplace_tag(prod: dict) -> bool:
    return "fireplace" in (prod.get("tags", "") or "").lower()

def _norm_images(prod: dict) -> List[str]:
    """Extract all image URLs from a product (no artificial limit)."""
    return [img.get("src") for img in (prod.get("images") or []) if img.get("src")]

def compute_hash(prod: dict) -> str:
    body = {
        "title": prod.get("title", ""),
        "body_html": prod.get("body_html", ""),
        "tags": prod.get("tags", ""),
        "status": prod.get("status", ""),
        "handle": prod.get("handle", ""),
        "variants": [
            {
                "sku": v.get("sku"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "opt1": v.get("option1"),
                "opt2": v.get("option2"),
                "opt3": v.get("option3"),
            }
            for v in (prod.get("variants") or [])
        ],
        "images": _norm_images(prod),
    }
    import hashlib
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()

def compute_hash_with_media(store: dict, prod: dict, pid: str | int) -> str:
    """Like compute_hash but adds stable video fingerprint (external embed URLs + hosted media IDs)."""
    body = {
        "title": prod.get("title", ""),
        "body_html": prod.get("body_html", ""),
        "tags": prod.get("tags", ""),
        "status": prod.get("status", ""),
        "handle": prod.get("handle", ""),
        "variants": [
            {
                "sku": v.get("sku"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "opt1": v.get("option1"),
                "opt2": v.get("option2"),
                "opt3": v.get("option3"),
            }
            for v in (prod.get("variants") or [])
        ],
        "images": _norm_images(prod),
        "videos": {"external": [], "hosted_ids": []},
    }
    try:
        media = list_media(store["domain"], store["token"], pid) or []
        for m in media:
            t = (m.get("media_type") or m.get("type") or "").lower()
            if t in ("external_video", "external-video"):
                eu = (m.get("external_video", {}).get("embed_url") or "").strip()
                if eu:
                    body["videos"]["external"].append(eu)
            elif t == "video":
                mid = m.get("id")
                if mid:
                    body["videos"]["hosted_ids"].append(str(mid))
        body["videos"]["external"].sort()
        body["videos"]["hosted_ids"].sort()
    except Exception:
        pass
    import hashlib
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()

# =========================================================
# GraphQL helpers
# =========================================================
FIND_BY_SKU = """
query($q:String!) {
  productVariants(first: 25, query: $q) {
    edges { node { id sku product { id title status tags handle } } }
  }
}
"""

PRODUCT_BY_HANDLE = """
query($handle:String!) {
  productByHandle(handle:$handle) { id title handle }
}
"""

PRODUCT_CREATE_MEDIA = """
mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
  productCreateMedia(productId: $productId, media: $media) {
    media { alt mediaContentType status }
    mediaUserErrors { field message }
  }
}
"""

STAGED_UPLOADS_CREATE = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
    userErrors { field message }
  }
}
"""

MEDIA_QUERY = """
query($id: ID!, $n: Int!) {
  product(id: $id) {
    id
    media(first: $n) {
      nodes {
        mediaContentType
        status
        ... on Video { sources { url mimeType } }
        ... on ExternalVideo { embeddedUrl }
      }
    }
  }
}
"""

def find_variant_by_sku(domain: str, token: str, sku: str):
    if not sku:
        return None, None
    data = graphql(domain, token, FIND_BY_SKU, {"q": f"sku:{sku}"})
    edges = data.get("data", {}).get("productVariants", {}).get("edges", []) or []
    if not edges:
        return None, None
    node = edges[0]["node"]
    return node["product"]["id"], node["id"]

def _duplicate_sku_exists(domain: str, token: str, sku: str) -> bool:
    """True if that SKU appears on 2+ different products on the destination store."""
    if not sku:
        return False
    data = graphql(domain, token, FIND_BY_SKU, {"q": f"sku:{sku}"})
    edges = data.get("data", {}).get("productVariants", {}).get("edges", []) or []
    prod_ids = set()
    for e in edges:
        pid = (((e or {}).get("node") or {}).get("product") or {}).get("id")
        if pid:
            prod_ids.add(pid)
    return len(prod_ids) > 1

def find_product_id_by_any_sku(domain: str, token: str, skus: list[str]) -> Optional[str]:
    for sku in skus:
        if not sku:
            continue
        prod_gid, _ = find_variant_by_sku(domain, token, sku)
        if prod_gid:
            return prod_gid.split("/")[-1]
    return None

def find_product_id_by_handle(domain: str, token: str, handle: str) -> Optional[str]:
    if not handle:
        return None
    resp = graphql(domain, token, PRODUCT_BY_HANDLE, {"handle": handle})
    node = (resp.get("data", {}) or {}).get("productByHandle")
    if node and node.get("id"):
        return node["id"].split("/")[-1]
    return None

# =========================================================
# REST helpers (products, variants, metafields)
# =========================================================

def get_product_with_retry(domain: str, token: str, pid: int | str, attempts: int = 4, delay: float = 0.8) -> Optional[dict]:
    """Helps when Shopify fires webhook before duplicate is fully readable."""
    for _ in range(attempts):
        p = get_product(domain, token, pid)
        if p:
            return p
        time.sleep(delay)
    return None

def get_product(domain: str, token: str, pid: int | str) -> Optional[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("product")
    return None

def list_metafields(domain: str, token: str, pid: int | str) -> list[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}/metafields.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("metafields", [])
    return []

def get_mf(domain: str, token: str, pid: int | str, ns: str, key: str) -> Optional[str]:
    for m in list_metafields(domain, token, pid):
        if m.get("namespace") == ns and m.get("key") == key:
            return m.get("value")
    return None

def get_mf_id(domain: str, token: str, pid: int | str, ns: str, key: str) -> Optional[int]:
    for m in list_metafields(domain, token, pid):
        if m.get("namespace") == ns and m.get("key") == key:
            return m.get("id")
    return None

def set_mf(domain: str, token: str, pid: int | str, ns: str, key: str, type_: str, value: str):
    payload = {"metafield": {
        "namespace": ns, "key": key, "type": type_, "value": value,
        "owner_resource": "product", "owner_id": pid
    }}
    requests.post(f"{admin_base(domain)}/metafields.json",
                  headers=rest_headers(token), json=payload, timeout=25)

def delete_mf(domain: str, token: str, mf_id: int):
    requests.delete(f"{admin_base(domain)}/metafields/{mf_id}.json",
                    headers=rest_headers(token), timeout=20)

def delete_product(domain: str, token: str, pid: str):
    requests.delete(f"{admin_base(domain)}/products/{pid}.json",
                    headers=rest_headers(token), timeout=25)

def _set_product_status(domain: str, token: str, pid: str | int, status: str) -> bool:
    payload = {"product": {"id": int(pid), "status": status}}
    r = requests.put(f"{admin_base(domain)}/products/{pid}.json",
                     headers=rest_headers(token), json=payload, timeout=25)
    if r.status_code in (200, 201):
        info(f"[status] set {domain} PID {pid} -> {status}")
        return True
    warn(f"[status] failed setting {pid} to {status} on {domain}: {r.status_code} {r.text}")
    return False

def list_images(domain: str, token: str, pid: str | int) -> list[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}/images.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("images", [])
    return []

def delete_all_images(domain: str, token: str, pid: str | int):
    for img in list_images(domain, token, pid):
        iid = img.get("id")
        if not iid:
            continue
        requests.delete(f"{admin_base(domain)}/products/{pid}/images/{iid}.json",
                        headers=rest_headers(token), timeout=25)

class ImageUploadError(Exception):
    """Raised when image upload fails with a retryable error."""
    pass

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(ImageUploadError),
)
def add_image(domain: str, token: str, pid: str | int, src: str, position: int, alt: Optional[str] = None):
    payload = {"image": {"src": src, "position": position}}
    if alt:
        payload["image"]["alt"] = alt
    r = requests.post(f"{admin_base(domain)}/products/{pid}/images.json",
                      headers=rest_headers(token), json=payload, timeout=90)
    if r.status_code in (429, 502, 503, 504):
        # Retryable errors - rate limit or server errors
        raise ImageUploadError(f"Retryable error {r.status_code}: {r.text}")
    if r.status_code not in (200, 201):
        warn(f"[media] add_image failed on {domain} pid={pid} pos={position}: {r.status_code} {r.text}")

def list_media(domain: str, token: str, pid: str | int) -> list[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}/media.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("media", [])
    return []

def delete_all_media(domain: str, token: str, pid: str | int):
    for m in list_media(domain, token, pid):
        mid = m.get("id")
        if not mid:
            continue
        requests.delete(f"{admin_base(domain)}/products/{pid}/media/{mid}.json",
                        headers=rest_headers(token), timeout=25)

def create_media_external_video(domain: str, token: str, pid: str | int, embed_url: str, alt: Optional[str] = None):
    payload = {"media": [{"alt": alt or "", "external_video": {"embed_url": embed_url}}]}
    r = requests.post(f"{admin_base(domain)}/products/{pid}/media.json",
                      headers=rest_headers(token), json=payload, timeout=90)
    if r.status_code not in (200, 201):
        warn(f"[media] create_external_video failed on {domain} pid={pid}: {r.status_code} {r.text}")

# =========================================================
# Cross-link helpers
# =========================================================
def get_cross_link_pid_on_dst(src_store: dict, dst_store: dict, src_pid: str | int) -> Optional[str]:
    key = "afl_product_id" if dst_store["name"] == "AFL" else "taf_product_id"
    val = get_mf(src_store["domain"], src_store["token"], src_pid, "sync", key)
    if not val:
        return None
    if isinstance(val, str):
        if val.isdigit():
            return val
        if "gid://shopify/Product/" in val:
            return val.split("/")[-1]
        return str(val)
    return str(val)

def write_cross_links(src_store: dict, dst_store: dict, src_pid: str | int, dst_pid: str | int):
    set_mf(src_store["domain"], src_store["token"], src_pid, "sync",
           "afl_product_id" if dst_store["name"] == "AFL" else "taf_product_id",
           "single_line_text_field", str(dst_pid))
    set_mf(dst_store["domain"], dst_store["token"], dst_pid, "sync",
           "taf_product_id" if src_store["name"] == "TAF" else "afl_product_id",
           "single_line_text_field", str(src_pid))

def product_exists(domain: str, token: str, pid: str | int) -> bool:
    try:
        p = get_product(domain, token, pid)
        return bool(p and p.get("id"))
    except Exception:
        return False

def clear_cross_link_on_src(src_store: dict, src_pid: str | int, dst_store_name: str):
    key = "afl_product_id" if dst_store_name == "AFL" else "taf_product_id"
    mfid = get_mf_id(src_store["domain"], src_store["token"], src_pid, "sync", key)
    if mfid:
        delete_mf(src_store["domain"], src_store["token"], mfid)

def draft_on_dst_by_handle_or_crosslink(src_store: dict, dst_store: dict, src_prod: dict, src_pid: str | int) -> bool:
    # cross-link
    dst_pid = get_cross_link_pid_on_dst(src_store, dst_store, src_pid)
    if dst_pid and product_exists(dst_store["domain"], dst_store["token"], dst_pid):
        info(f"[status] drafting {dst_store['name']} PID {dst_pid} (by cross-link)")
        return _set_product_status(dst_store["domain"], dst_store["token"], dst_pid, "draft")
    # handle
    handle = (src_prod.get("handle") or "").strip()
    if handle:
        found = find_product_id_by_handle(dst_store["domain"], dst_store["token"], handle)
        if found and product_exists(dst_store["domain"], dst_store["token"], found):
            info(f"[status] drafting {dst_store['name']} PID {found} (by handle '{handle}')")
            return _set_product_status(dst_store["domain"], dst_store["token"], found, "draft")
    # any SKU
    skus = [ (v.get("sku") or "").strip() for v in (src_prod.get("variants") or []) if (v.get("sku") or "").strip() ]
    if skus:
        found = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], skus)
        if found and product_exists(dst_store["domain"], dst_store["token"], found):
            info(f"[status] drafting {dst_store['name']} PID {found} (by SKU match)")
            return _set_product_status(dst_store["domain"], dst_store["token"], found, "draft")
    return False

# =========================================================
# Retry wrappers for product PUT/POST
# =========================================================
class TransientWriteError(Exception): ...
class RateLimit(Exception): ...

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    retry=retry_if_exception_type(TransientWriteError),
)
def _put_product_with_retry(domain: str, token: str, product_payload: dict) -> dict:
    r = requests.put(f"{admin_base(domain)}/products/{product_payload['product']['id']}.json",
                     headers=rest_headers(token), json=product_payload, timeout=40)
    if r.status_code in (200, 201):
        return r.json().get("product")
    if r.status_code in (409, 429, 502, 503):
        raise TransientWriteError(r.text)
    raise RuntimeError(f"PUT product failed {r.status_code}: {r.text}")

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    retry=retry_if_exception_type(TransientWriteError),
)
def _post_product_with_retry(domain: str, token: str, product_payload: dict) -> dict:
    r = requests.post(f"{admin_base(domain)}/products.json",
                      headers=rest_headers(token), json=product_payload, timeout=40)
    if r.status_code in (200, 201):
        return r.json().get("product")
    if r.status_code in (409, 429, 502, 503):
        raise TransientWriteError(r.text)
    raise RuntimeError(f"POST product failed {r.status_code}: {r.text}")

# =========================================================
# Inventory helpers (with no-op suppression)
# =========================================================
def _primary_location_id(domain: str, token: str) -> Optional[str]:
    r = requests.get(f"{admin_base(domain)}/locations.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        return None
    locs = r.json().get("locations", [])
    if not locs:
        return None
    for l in locs:
        if l.get("primary"):
            return str(l.get("id"))
    return str(locs[0].get("id"))

def _variant_inventory_item_id(domain: str, token: str, variant_id: str | int) -> Optional[int]:
    r = requests.get(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        return None
    return (r.json().get("variant") or {}).get("inventory_item_id")

def _sum_available_for_item(domain: str, token: str, inventory_item_id: int, location_id_legacy: Optional[str]) -> int:
    params = {"inventory_item_ids": inventory_item_id}
    if location_id_legacy:
        params["location_ids"] = location_id_legacy
    r = requests.get(f"{admin_base(domain)}/inventory_levels.json",
                     headers=rest_headers(token), params=params, timeout=20)
    if r.status_code != 200:
        return 0
    levels = r.json().get("inventory_levels", []) or []
    return sum((lvl.get("available") or 0) for lvl in levels)

def _current_available_for_item(domain: str, token: str, inventory_item_id: int, location_id_legacy: Optional[str]) -> Optional[int]:
    params = {"inventory_item_ids": inventory_item_id}
    if location_id_legacy:
        params["location_ids"] = location_id_legacy
    r = requests.get(f"{admin_base(domain)}/inventory_levels.json",
                     headers=rest_headers(token), params=params, timeout=20)
    if r.status_code != 200:
        return None
    levels = r.json().get("inventory_levels", []) or []
    return sum((lvl.get("available") or 0) for lvl in levels)

def product_total_available(domain: str, token: str, prod: dict, enforce_location: Optional[str]) -> int:
    total = 0
    for v in (prod.get("variants") or []):
        vid = v.get("id")
        if not vid:
            continue
        inv_item = _variant_inventory_item_id(domain, token, vid)
        if not inv_item:
            continue
        total += _sum_available_for_item(domain, token, inv_item, enforce_location)
    return total

def _connect_level_if_needed(domain: str, token: str, inventory_item_id: int, location_id_legacy: str):
    payload = {"inventory_item_id": int(inventory_item_id), "location_id": int(location_id_legacy)}
    requests.post(f"{admin_base(domain)}/inventory_levels/connect.json",
                  headers=rest_headers(token), json=payload, timeout=20)

# in-memory suppression of identical sets
_INVENTORY_SET_CACHE: dict[tuple[str, str], tuple[int, float]] = {}
_INVENTORY_SET_TTL = 30

def _recently_set_same_qty(domain: str, sku: str, qty: int) -> bool:
    key = (domain, sku)
    now = time.time()
    # gc
    for k, (q, ts) in list(_INVENTORY_SET_CACHE.items()):
        if now - ts > _INVENTORY_SET_TTL:
            _INVENTORY_SET_CACHE.pop(k, None)
    if key in _INVENTORY_SET_CACHE and _INVENTORY_SET_CACHE[key][0] == int(qty) and (now - _INVENTORY_SET_CACHE[key][1]) < _INVENTORY_SET_TTL:
        return True
    _INVENTORY_SET_CACHE[key] = (int(qty), now)
    return False

def ensure_tracking_enabled_on_product(domain: str, token: str, pid: str | int):
    r = requests.get(f"{admin_base(domain)}/products/{pid}.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code != 200:
        return
    product = r.json().get("product") or {}
    for v in (product.get("variants") or []):
        vid = v.get("id")
        if not vid:
            continue
        if not v.get("inventory_management"):
            pu = {"variant": {"id": vid, "inventory_management": "shopify"}}
            rr = requests.put(f"{admin_base(domain)}/variants/{vid}.json",
                              headers=rest_headers(token), json=pu, timeout=25)
            if rr.status_code not in (200, 201):
                warn(f"[inventory] enable tracking failed on {domain} variant {vid}: {rr.status_code} {rr.text}")

def set_absolute_inventory_by_sku(domain: str, token: str, sku: str, qty: int, location_id_legacy: Optional[str]):
    if qty is None or not sku:
        return
    _, var_gid = find_variant_by_sku(domain, token, sku)
    if not var_gid:
        return
    variant_id = var_gid.split("/")[-1]
    r = requests.get(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        return
    variant = (r.json().get("variant") or {})
    inventory_item_id = variant.get("inventory_item_id")
    if not inventory_item_id:
        return

    if not location_id_legacy:
        location_id_legacy = _primary_location_id(domain, token)
    if not location_id_legacy:
        return

    # Skip if equal (no-op) and suppress recent identical sets
    current = _current_available_for_item(domain, token, inventory_item_id, location_id_legacy)
    if current is not None and int(current) == int(qty):
        debug(f"[inventory] noop set skipped for {sku} on {domain}: {current} == {qty}")
        return
    if _recently_set_same_qty(domain, sku, qty):
        debug(f"[inventory] recent identical set suppressed for {sku} on {domain}: {qty}")
        return

    _connect_level_if_needed(domain, token, inventory_item_id, location_id_legacy)
    payload = {"location_id": int(location_id_legacy),
               "inventory_item_id": int(inventory_item_id),
               "available": int(qty)}
    rr = requests.post(f"{admin_base(domain)}/inventory_levels/set.json",
                       headers=rest_headers(token), json=payload, timeout=20)
    if rr.status_code not in (200, 201):
        warn(f"[inventory] set failed for {sku} on {domain}: {rr.status_code} {rr.text}")

def adjust_inventory_by_sku(domain: str, token: str, sku: str, delta: int, location_id_legacy: Optional[str]):
    if not sku or not delta:
        return
    _, var_gid = find_variant_by_sku(domain, token, sku)
    if not var_gid:
        warn(f"[inventory] No variant for SKU {sku} on {domain}")
        return
    variant_id = var_gid.split("/")[-1]
    r = requests.get(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        warn(f"[inventory] Could not read variant {variant_id} on {domain}: {r.text}")
        return
    inventory_item_id = (r.json().get("variant") or {}).get("inventory_item_id")
    if not inventory_item_id:
        return
    if not location_id_legacy:
        location_id_legacy = _primary_location_id(domain, token)
    if not location_id_legacy:
        return
    adj = {"location_id": int(location_id_legacy),
           "inventory_item_id": int(inventory_item_id),
           "available_adjustment": int(delta)}
    rr = requests.post(f"{admin_base(domain)}/inventory_levels/adjust.json",
                       headers=rest_headers(token), json=adj, timeout=20)
    if rr.status_code not in (200, 201):
        warn(f"[inventory] Adjust failed for {sku} on {domain}: {rr.status_code} {rr.text}")

def get_sku_from_inventory_item_id(domain: str, token: str, inventory_item_id: int | str) -> Optional[str]:
    """Look up the SKU for a given inventory_item_id by querying the inventory item."""
    r = requests.get(f"{admin_base(domain)}/inventory_items/{inventory_item_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        warn(f"[inventory] Could not read inventory_item {inventory_item_id} on {domain}: {r.text}")
        return None
    inv_item = r.json().get("inventory_item") or {}
    sku = (inv_item.get("sku") or "").strip()
    return sku if sku else None

def handle_inventory_level_update(src_store: dict, dst_store: dict, payload: dict):
    """
    Handle inventory_levels/update webhook from src_store, sync to dst_store.
    Payload contains: inventory_item_id, location_id, available, updated_at
    """
    inventory_item_id = payload.get("inventory_item_id")
    available = payload.get("available")
    location_id = payload.get("location_id")
    
    if inventory_item_id is None or available is None:
        debug(f"[inventory] Missing inventory_item_id or available in payload")
        return
    
    # Check if this location matches our configured location (if configured)
    src_location = src_store.get("location_id")
    if src_location and str(location_id) != str(src_location):
        debug(f"[inventory] Ignoring inventory update for location {location_id} (configured: {src_location})")
        return
    
    # Look up SKU from inventory_item_id
    sku = get_sku_from_inventory_item_id(src_store["domain"], src_store["token"], inventory_item_id)
    if not sku:
        debug(f"[inventory] No SKU found for inventory_item_id {inventory_item_id}")
        return
    
    info(f"[inventory] {src_store['name']} -> {dst_store['name']}: SKU={sku} available={available}")
    
    # Set absolute inventory on destination store
    set_absolute_inventory_by_sku(
        dst_store["domain"], dst_store["token"], 
        sku, int(available), 
        dst_store.get("location_id")
    )

def mirror_inventory_values_from_src_to_dst(
    src_store: dict,
    dst_store: dict,
    src_prod: dict,
    src_location_legacy: Optional[str],
    dst_location_legacy: Optional[str],
):
    dst_pid = find_product_id_by_any_sku(
        dst_store["domain"], dst_store["token"],
        [v.get("sku") for v in (src_prod.get("variants") or []) if v.get("sku")]
    )
    if dst_pid:
        ensure_tracking_enabled_on_product(dst_store["domain"], dst_store["token"], dst_pid)
    for v in src_prod.get("variants", []):
        sku = v.get("sku")
        if not sku:
            continue
        _, var_gid = find_variant_by_sku(src_store["domain"], src_store["token"], sku)
        if not var_gid:
            continue
        src_variant_id = var_gid.split("/")[-1]
        r = requests.get(f"{admin_base(src_store['domain'])}/variants/{src_variant_id}.json",
                         headers=rest_headers(src_store["token"]), timeout=20)
        if r.status_code != 200:
            continue
        inv_item_id = (r.json().get("variant") or {}).get("inventory_item_id")
        if not inv_item_id:
            continue
        if not src_location_legacy:
            src_location_legacy = _primary_location_id(src_store["domain"], src_store["token"])
        params = {"inventory_item_ids": inv_item_id}
        if src_location_legacy:
            params["location_ids"] = src_location_legacy
        lv = requests.get(f"{admin_base(src_store['domain'])}/inventory_levels.json",
                          headers=rest_headers(src_store["token"]), params=params, timeout=20)
        if lv.status_code != 200:
            available = 0
        else:
            levels = lv.json().get("inventory_levels", []) or []
            available = sum((x.get("available") or 0) for x in levels)
        info(f"[inventory] mirror {sku}: {available} {src_store['name']} -> {dst_store['name']}")
        set_absolute_inventory_by_sku(dst_store["domain"], dst_store["token"], sku, available, dst_location_legacy)

# =========================================================
# Price sync helpers (with throttle/retry)
# =========================================================
_last_call_ts = {"AFL": 0.0, "TAF": 0.0}

def _throttle(store_name: str, min_interval=0.6):
    now = time.time()
    wait_for = min_interval - (now - _last_call_ts.get(store_name, 0.0))
    if wait_for > 0:
        time.sleep(wait_for)
    _last_call_ts[store_name] = time.time()

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.8, min=0.8, max=6),
    retry=retry_if_exception_type(RateLimit),
)
def update_variant_price_by_sku(domain: str, token: str, sku: str,
                                price: Optional[str], compare_at: Optional[str], *,
                                store_name: str):
    if not sku:
        return
    _throttle(store_name, min_interval=0.6)

    # Safeguard: skip if duplicate SKU exists on destination
    if _duplicate_sku_exists(domain, token, sku):
        warn(f"[price] duplicate SKU '{sku}' on {domain}; skipping price update to avoid cross-product mismatch")
        return

    _, var_gid = find_variant_by_sku(domain, token, sku)
    if not var_gid:
        return
    dst_vid = var_gid.split("/")[-1]
    payload = {"variant": {"id": int(dst_vid)}}
    if price is not None:
        payload["variant"]["price"] = price
    if compare_at is not None:
        payload["variant"]["compare_at_price"] = compare_at
    r = requests.put(f"{admin_base(domain)}/variants/{dst_vid}.json",
                     headers=rest_headers(token), json=payload, timeout=25)
    if r.status_code == 429:
        raise RateLimit("429: price PUT")
    if r.status_code not in (200, 201):
        warn(f"[price] update failed for SKU {sku} on {domain}: {r.status_code} {r.text}")

def sync_variant_prices_from_src_to_dst(src_prod: dict, dst_store: dict):
    for v in (src_prod.get("variants") or []):
        sku = v.get("sku")
        if not sku:
            continue
        update_variant_price_by_sku(
            dst_store["domain"], dst_store["token"],
            sku, v.get("price"), v.get("compare_at_price"),
            store_name=dst_store["name"]
        )

# =========================================================
# SKU late-add helpers
# =========================================================
def get_product_with_variants(domain: str, token: str, pid: str | int) -> Optional[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("product")
    return None

def _variant_key(v: dict) -> tuple:
    return (
        (v.get("option1") or "").strip().lower(),
        (v.get("option2") or "").strip().lower(),
        (v.get("option3") or "").strip().lower(),
        int(v.get("position") or 0),
    )

def find_dst_variant_id_by_options(domain: str, token: str, dst_pid: str | int, src_variant: dict) -> Optional[str]:
    dst_prod = get_product_with_variants(domain, token, dst_pid)
    if not dst_prod:
        return None
    skey = _variant_key(src_variant)
    for dv in (dst_prod.get("variants") or []):
        if _variant_key(dv) == skey:
            return str(dv.get("id")) if dv.get("id") else None
    return None

def put_variant_fields(domain: str, token: str, variant_id: str | int, *,
                       sku: Optional[str] = None,
                       price: Optional[str] = None,
                       compare_at_price: Optional[str] = None):
    payload = {"variant": {"id": int(variant_id)}}
    if sku is not None:
        payload["variant"]["sku"] = sku
    if price is not None:
        payload["variant"]["price"] = price
    if compare_at_price is not None:
        payload["variant"]["compare_at_price"] = compare_at_price
    r = requests.put(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), json=payload, timeout=25)
    if r.status_code not in (200, 201):
        warn(f"[variant] update failed id={variant_id} on {domain}: {r.status_code} {r.text}")

def sync_variant_skus_from_src_to_dst_by_options(src_prod: dict, dst_store: dict, dst_pid: str | int,
                                                 also_sync_price: bool = False):
    variants = src_prod.get("variants") or []
    if not variants:
        return
    for sv in variants:
        sku = (sv.get("sku") or "").strip()
        dst_vid = find_dst_variant_id_by_options(dst_store["domain"], dst_store["token"], dst_pid, sv)
        if not dst_vid:
            continue
        r = requests.get(f"{admin_base(dst_store['domain'])}/variants/{dst_vid}.json",
                         headers=rest_headers(dst_store["token"]), timeout=20)
        if r.status_code != 200:
            continue
        dv = (r.json().get("variant") or {})
        dst_sku = (dv.get("sku") or "").strip()
        need_sku_update = bool(sku) and (sku != dst_sku)

        p = sv.get("price")
        cap = sv.get("compare_at_price")
        need_price_update = False
        if also_sync_price:
            need_price_update = (p is not None and p != dv.get("price")) or \
                                (cap is not None and cap != dv.get("compare_at_price"))

        if need_sku_update or need_price_update:
            put_variant_fields(dst_store["domain"], dst_store["token"], dst_vid,
                               sku=sku if need_sku_update else None,
                               price=p if (also_sync_price and need_price_update) else None,
                               compare_at_price=cap if (also_sync_price and need_price_update) else None)

# =========================================================
# Media sync (images only; videos optional/disabled)
# =========================================================
def _list_image_urls(domain: str, token: str, pid: str | int) -> list[str]:
    imgs = list_images(domain, token, pid) or []
    imgs = sorted(imgs, key=lambda x: int(x.get("position") or 0))
    return [(im.get("src") or "").strip() for im in imgs]

def _src_images_with_alt(prod: dict) -> list[tuple[str, str]]:
    """Extract all images with alt text from a product (no artificial limit)."""
    out = []
    for im in (prod.get("images") or []):
        src = (im.get("src") or "").strip()
        if not src:
            continue
        out.append((src, (im.get("alt") or "").strip()))
    return out

def _sync_images(src_store: dict, dst_store: dict, src_prod: dict, dst_pid: str | int):
    # images only flow TAF -> AFL
    if src_store["name"] != "TAF":
        return
    src_imgs = _src_images_with_alt(src_prod)
    if not src_imgs:
        info(f"[media] no images on source; skipping image sync for dst pid {dst_pid}")
        return
    try:
        dst_urls = _list_image_urls(dst_store["domain"], dst_store["token"], dst_pid)
        src_urls = [u for (u, _alt) in src_imgs]
        if dst_urls == src_urls:
            debug(f"[media] images already in sync for dst pid {dst_pid}; skipping")
            return
        info(f"[media] syncing {len(src_imgs)} images {src_store['name']} -> {dst_store['name']} (dst pid {dst_pid})")
        delete_all_images(dst_store["domain"], dst_store["token"], dst_pid)
        for idx, (src, alt) in enumerate(src_imgs, start=1):
            add_image(dst_store["domain"], dst_store["token"], dst_pid, src, idx, alt=alt or None)
            # Throttle between image uploads to avoid Shopify rate limiting
            if idx < len(src_imgs):
                time.sleep(0.5)
    except Exception as e:
        warn(f"[media] image sync error: {e}")

def list_video_media_only(domain: str, token: str, pid: str | int) -> list[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}/media.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code != 200:
        return []
    items = r.json().get("media", []) or []
    out = []
    for m in items:
        t = (m.get("media_type") or m.get("type") or "").lower()
        if t in ("video", "external_video", "external-video"):
            out.append(m)
    return out

def delete_video_media_only(domain: str, token: str, pid: str | int):
    for m in list_video_media_only(domain, token, pid):
        mid = m.get("id")
        if not mid:
            continue
        requests.delete(f"{admin_base(domain)}/products/{pid}/media/{mid}.json",
                        headers=rest_headers(token), timeout=25)

# --- Video helpers (kept but behind toggle) ---
def _get_media_urls_graphql(domain: str, token: str, pid: str | int, first: int = 30) -> Tuple[List[str], List[str], List[str]]:
    try:
        gid = f"gid://shopify/Product/{pid}"
        resp = graphql(domain, token, MEDIA_QUERY, {"id": gid, "n": first})
        nodes = (((resp.get("data") or {}).get("product") or {}).get("media") or {}).get("nodes") or []
        hosted, external, statuses = [], [], []
        for m in nodes:
            mtype = (m.get("mediaContentType") or "").upper()
            statuses.append((m.get("status") or "").upper())
            if mtype == "VIDEO":
                srcs = m.get("sources") or []
                for s in srcs:
                    url = s.get("url")
                    if url:
                        hosted.append(url)
                hosted = list(dict.fromkeys(hosted))
            elif mtype == "EXTERNAL_VIDEO":
                eu = (m.get("embeddedUrl") or "").strip()
                if eu:
                    external.append(eu)
        return hosted, external, statuses
    except Exception as e:
        warn(f"[media] GraphQL read failed: {e}")
        return [], [], []

def _get_media_urls_rest(domain: str, token: str, pid: str | int) -> Tuple[List[str], List[str], List[str]]:
    try:
        r = requests.get(f"{admin_base(domain)}/products/{pid}/media.json",
                         headers=rest_headers(token), timeout=25)
        if r.status_code != 200:
            return [], [], []
        items = r.json().get("media", []) or []
        hosted, external, statuses = [], [], []
        for m in items:
            t = (m.get("media_type") or m.get("type") or "").lower()
            statuses.append((m.get("status") or "").upper())
            if t == "video":
                for s in (m.get("sources") or []):
                    u = s.get("url")
                    if u:
                        hosted.append(u)
            elif t in ("external_video", "external-video"):
                eu = (m.get("external_video", {}).get("embed_url") or "").strip()
                if eu:
                    external.append(eu)
        return hosted, external, statuses
    except Exception:
        return [], [], []

def _get_media_urls(domain: str, token: str, pid: str | int) -> Tuple[List[str], List[str]]:
    hosted, external, statuses = _get_media_urls_graphql(domain, token, pid)
    if not hosted and not external:
        hosted, external, statuses = _get_media_urls_rest(domain, token, pid)
    decide(f"media-read: hosted={len(hosted)} external={len(external)} statuses={statuses}")
    return hosted, external

def _any_videos_on_product(domain: str, token: str, pid: str | int) -> bool:
    hosted, external, _ = _get_media_urls_graphql(domain, token, pid)
    if hosted or external:
        return True
    hosted, external, _ = _get_media_urls_rest(domain, token, pid)
    return bool(hosted or external)

def _wait_for_videos_on_source(src_store: dict, pid: str | int, max_wait_sec: int = 60, interval_sec: int = 4) -> Tuple[List[str], List[str]]:
    hosted, external = _get_media_urls(src_store["domain"], src_store["token"], pid)
    if hosted or external:
        return hosted, external
    if not _any_videos_on_product(src_store["domain"], src_store["token"], pid):
        return hosted, external
    decide(f"media: videos present but not ready; polling up to {max_wait_sec}s")
    t0 = time.time()
    while time.time() - t0 < max_wait_sec:
        time.sleep(interval_sec)
        hosted, external = _get_media_urls(src_store["domain"], src_store["token"], pid)
        if hosted or external:
            break
    return hosted, external

def _staged_upload_create_video(domain: str, token: str, filename: str, file_size: int, mime: str = "video/mp4") -> tuple[str, str, list[dict]]:
    variables = {
        "input": [
            {"resource": "VIDEO", "filename": filename, "mimeType": mime, "fileSize": int(file_size), "httpMethod": "POST"}
        ]
    }
    resp = graphql(domain, token, STAGED_UPLOADS_CREATE, variables)
    data = (resp.get("data") or {}).get("stagedUploadsCreate") or {}
    errs = data.get("userErrors") or []
    if errs:
        for e in errs:
            warn(f"[media] stagedUploadsCreate error on {domain}: {e.get('message')}")
        raise RuntimeError("stagedUploadsCreate failed")
    target = (data.get("stagedTargets") or [])[0]
    return target["url"], target["resourceUrl"], target["parameters"]

def _staged_upload_post(url: str, params: list[dict], file_bytes: bytes):
    form = {p["name"]: p["value"] for p in params if p.get("name") and p.get("value")}
    files = {"file": (form.get("key", "upload.mp4"), file_bytes)}
    r = requests.post(url, data=form, files=files, timeout=300)
    if r.status_code not in (204, 201, 200):
        raise RuntimeError(f"staged upload failed {r.status_code}: {r.text}")

def _download_bytes(url: str) -> tuple[bytes, str, str]:
    rr = requests.get(url, stream=True, timeout=120)
    rr.raise_for_status()
    data = rr.content
    mime = rr.headers.get("Content-Type", "video/mp4") or "video/mp4"
    import mimetypes
    ext = mimetypes.guess_extension(mime) or ".mp4"
    return data, ext, mime

def _product_create_media_videos(domain: str, token: str, dst_pid: str | int, video_urls: List[str]):
    if not video_urls:
        return
    product_gid = f"gid://shopify/Product/{dst_pid}"
    media = [{"alt": "", "originalSource": u, "mediaContentType": "VIDEO"} for u in video_urls]
    resp = graphql(domain, token, PRODUCT_CREATE_MEDIA, {"productId": product_gid, "media": media})
    errs = (resp.get("data", {}) or {}).get("productCreateMedia", {}).get("mediaUserErrors", [])
    if errs:
        for e in errs:
            warn(f"[media] productCreateMedia error on {domain}: {e.get('message')} field={e.get('field')}")

def _sync_videos(src_store: dict, dst_store: dict, src_pid: str | int, dst_pid: str | int):
    """No-op when disabled; kept for completeness."""
    if DISABLE_VIDEOS:
        decide("media: video sync disabled by config")
        return
    if src_store["name"] != "TAF":
        return
    try:
        hosted, external = _wait_for_videos_on_source(src_store, src_pid, max_wait_sec=60, interval_sec=4)
        if not hosted and not external:
            decide("media: no videos to sync (0 hosted, 0 external)")
            return

        info(f"[media] syncing videos (hosted:{len(hosted)} external:{len(external)}) "
             f"{src_store['name']} -> {dst_store['name']} (dst pid {dst_pid})")

        delete_video_media_only(dst_store["domain"], dst_store["token"], dst_pid)

        for u in external[:10]:
            try:
                create_media_external_video(dst_store["domain"], dst_store["token"], dst_pid, u)
            except Exception as ex:
                warn(f"[media] external video attach failed on {dst_store['domain']} url={u}: {ex}")

        for idx, src_url in enumerate(hosted[:8], start=1):
            try:
                data, ext, mime = _download_bytes(src_url)
                filename = f"taf-video-{int(time.time())}-{idx}{ext or '.mp4'}"
                post_url, resource_url, params = _staged_upload_create_video(
                    dst_store["domain"], dst_store["token"], filename=filename, file_size=len(data), mime=mime
                )
                _staged_upload_post(post_url, params, data)
                _product_create_media_videos(dst_store["domain"], dst_store["token"], dst_pid, [resource_url])
            except Exception as inner:
                warn(f"[media] hosted video copy failed on {dst_store['domain']} src={src_url}: {inner}")

        t0 = time.time()
        while time.time() - t0 < 45:
            vids = list_video_media_only(dst_store["domain"], dst_store["token"], dst_pid)
            if not vids:
                time.sleep(2)
                continue
            statuses = [(m.get("status") or "").upper() for m in vids]
            if all(s in ("READY", "FAILED") for s in statuses):
                decide(f"media: destination video statuses={statuses}")
                break
            time.sleep(2)

    except Exception as e:
        warn(f"[media] video sync error: {e}")

# =========================================================
# Readiness helpers
# =========================================================
def _sku_ready(prod: dict) -> bool:
    return any((v.get("sku") or "").strip() for v in (prod.get("variants") or []))

# =========================================================
# Core sync handler
# =========================================================
def handle_product_event(src_store: dict, dst_store: dict, payload: dict):
    pid = payload.get("id")
    if not pid:
        return

    k = _key(src_store["name"], pid)
    if _muted(k):
        debug(f"[{src_store['name']} ➝ {dst_store['name']}] muted PID {pid}")
        return
    if _debounced(k):
        debug(f"[{src_store['name']} ➝ {dst_store['name']}] debounced PID {pid}")
        return

    lock = _lock_for(src_store["name"], pid)
    if not lock.acquire(blocking=False):
        return

    decide(f"{src_store['name']}→{dst_store['name']} pid={pid} webhook start")
    try:
        prod = get_product_with_retry(src_store["domain"], src_store["token"], pid)
        if not prod:
            decide(f"{src_store['name']} pid={pid} deleted on source")
            if not draft_on_dst_by_handle_or_crosslink(src_store, dst_store, {"handle": payload.get("handle", "")}, pid):
                decide("draft-skip: no match on delete")
            return

        # Echo suppression / clean stale origin
        origin = get_mf(src_store["domain"], src_store["token"], pid, ORIGIN_NS, ORIGIN_KEY)
        if src_store["name"] == "AFL":
            if origin and origin == dst_store["name"]:
                decide(f"skip: origin marker (origin={origin}) pid={pid}")
                return
        else:
            if origin in ("AFL", "TAF"):
                mfid = get_mf_id(src_store["domain"], src_store["token"], pid, ORIGIN_NS, ORIGIN_KEY)
                if mfid:
                    delete_mf(src_store["domain"], src_store["token"], mfid)
                decide(f"ignored & cleared stale origin on {src_store['name']}: {origin}")

        # Hard gate: only TAF drives product content/inventory mirroring
        if src_store["name"] == "AFL":
            decide("skip: AFL→TAF product content ignored (TAF is source of truth)")
            return

        # Hold switch while editing on TAF
        if has_hold_tag(prod):
            if HOLD_BEHAVIOUR == "draft":
                drafted = draft_on_dst_by_handle_or_crosslink(src_store, dst_store, prod, pid)
                decide(f"hold: tag present; drafted={drafted}")
            else:
                decide("hold: tag present; skipping all sync for this product")
            return

        # Filter: only 'fireplace' products participate; otherwise draft on dst
        if not has_fireplace_tag(prod):
            decide(f"draft: missing 'fireplace' tag pid={pid}")
            if not draft_on_dst_by_handle_or_crosslink(src_store, dst_store, prod, pid):
                decide("draft-skip: no cross-link/handle/SKU match")
            return

        # Availability (draft instead of delete)
        # Only force draft if TAF status is explicitly draft/archived
        # Don't draft solely based on inventory being 0 - inventory syncs separately
        force_draft = False
        taf_total_avail = product_total_available(src_store["domain"], src_store["token"], prod, src_store.get("location_id"))
        decide(f"availability: status={prod.get('status')} total_avail={taf_total_avail} pid={pid}")
        if prod.get("status") in ("draft", "archived"):
            force_draft = True
            decide("will draft on AFL (TAF status is draft/archived), but continue syncing fields")

        # Hash (include media on TAF)
        new_hash = compute_hash_with_media(src_store, prod, pid)
        last_hash = get_mf(src_store["domain"], src_store["token"], pid, "sync", "last_hash")
        decide(f"hash: last={str(last_hash)[:8] if last_hash else None} new={new_hash[:8]}")
        if last_hash and last_hash == new_hash:
            decide("skip: hash match")
            return

        # Resolve destination: cross-link -> handle -> any SKU
        dst_pid = get_cross_link_pid_on_dst(src_store, dst_store, pid)
        decide(f"resolve: cross-link => {dst_pid}")
        if dst_pid and not product_exists(dst_store["domain"], dst_store["token"], dst_pid):
            warn(f"[resolve] cross-link {dst_pid} missing on {dst_store['name']}")
            clear_cross_link_on_src(src_store, pid, dst_store["name"])
            dst_pid = None
        if not dst_pid and prod.get("handle"):
            dst_pid = find_product_id_by_handle(dst_store["domain"], dst_store["token"], prod.get("handle"))
            decide(f"resolve: handle '{prod.get('handle')}' => {dst_pid}")
        if not dst_pid:
            skus = [(v.get("sku") or "").strip() for v in (prod.get("variants") or []) if (v.get("sku") or "").strip()]
            if skus:
                dst_pid = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], skus)
                decide(f"resolve: by-any-sku {skus} => {dst_pid}")

        # Base payload
        product_payload = {
            "product": {
                "title": prod.get("title"),
                "body_html": prod.get("body_html", ""),
                "vendor": prod.get("vendor"),
                "product_type": prod.get("product_type"),
                "tags": prod.get("tags", ""),
                "status": prod.get("status", "active"),
                "handle": prod.get("handle"),
            }
        }
        created_now = False

        # If TAF is unavailable AND no counterpart exists yet, *skip creating* until it becomes available/SKU-ready
        if not dst_pid and force_draft and not _sku_ready(prod):
            decide("create-skip: TAF is draft/0 stock and no SKU yet; will create on next active/SKU-ready change")
            return

        if dst_pid:
            # UPDATE
            info(f"[{src_store['name']} ➝ {dst_store['name']}] updating PID {dst_pid}")
            product_payload["product"]["id"] = int(dst_pid)
            set_mf(dst_store["domain"], dst_store["token"], dst_pid,
                   ORIGIN_NS, ORIGIN_KEY, "single_line_text_field", src_store["name"])
            _put_product_with_retry(dst_store["domain"], dst_store["token"], product_payload)

            # Status decision
            sku_ready = _sku_ready(prod)
            decide(f"status-decision(update): force_draft={force_draft} sku_ready={sku_ready}")
            target_status = "draft" if (force_draft or not sku_ready) else "active"
            ok = _set_product_status(dst_store["domain"], dst_store["token"], dst_pid, target_status)
            decide(f"status={target_status} result={ok}")

            # Ensure SKUs, then mirror inventory & prices
            sync_variant_skus_from_src_to_dst_by_options(prod, dst_store, dst_pid, also_sync_price=False)
            if _sku_ready(prod):
                mirror_inventory_values_from_src_to_dst(
                    src_store, dst_store, prod, src_store.get("location_id"), dst_store.get("location_id")
                )
                sync_variant_prices_from_src_to_dst(prod, dst_store)

            _mute_for(_key(dst_store["name"], dst_pid), MUTE_SEC)

        else:
            # CREATE
            info(f"[{src_store['name']} ➝ {dst_store['name']}] creating new product")
            product_payload["product"]["variants"] = [
                {
                    "sku": v.get("sku"),
                    "price": v.get("price"),
                    "compare_at_price": v.get("compare_at_price"),
                    "taxable": v.get("taxable", True),
                    "requires_shipping": v.get("requires_shipping", True),
                    "barcode": v.get("barcode"),
                    "option1": v.get("option1"),
                    "option2": v.get("option2"),
                    "option3": v.get("option3"),
                    "inventory_management": "shopify",
                }
                for v in (prod.get("variants") or [])
            ]
            try:
                created = _post_product_with_retry(dst_store["domain"], dst_store["token"], product_payload)
                if created:
                    dst_pid = str(created["id"])
                    created_now = True
                    _mute_for(_key(dst_store["name"], dst_pid), MUTE_SEC)
            except RuntimeError as e:
                msg = str(e)
                if "422" in msg and "handle" in msg and prod.get("handle"):
                    hp = prod.get("handle")
                    found = find_product_id_by_handle(dst_store["domain"], dst_store["token"], hp)
                    if found:
                        info(f"[recover] handle '{hp}' taken on {dst_store['name']}, switching to update PID {found}")
                        dst_pid = found
                        product_payload["product"]["id"] = int(dst_pid)
                        set_mf(dst_store["domain"], dst_store["token"], dst_pid,
                               ORIGIN_NS, ORIGIN_KEY, "single_line_text_field", src_store["name"])
                        _put_product_with_retry(dst_store["domain"], dst_store["token"], product_payload)
                        _mute_for(_key(dst_store["name"], dst_pid), MUTE_SEC)
                    else:
                        raise
                else:
                    raise

            if not dst_pid:
                warn(f"[{src_store['name']} ➝ {dst_store['name']}] no dst PID after create/update, abort media/inventory")
                return

            sku_ready = _sku_ready(prod)
            decide(f"status-decision(create): force_draft={force_draft} sku_ready={sku_ready}")
            target_status = "draft" if (force_draft or not sku_ready) else "active"
            ok = _set_product_status(dst_store["domain"], dst_store["token"], dst_pid, target_status)
            decide(f"status={target_status} result={ok}")

            sync_variant_skus_from_src_to_dst_by_options(prod, dst_store, dst_pid, also_sync_price=False)
            if _sku_ready(prod):
                mirror_inventory_values_from_src_to_dst(
                    src_store, dst_store, prod, src_store.get("location_id"), dst_store.get("location_id")
                )
                sync_variant_prices_from_src_to_dst(prod, dst_store)

        # Media (images always; videos only if enabled)
        hosted, external = [], []
        if not DISABLE_VIDEOS:
            hosted, external = _wait_for_videos_on_source(src_store, pid, max_wait_sec=45, interval_sec=3)
        decide(f"media: images={len(_norm_images(prod))} hosted_videos={len(hosted)} external_videos={len(external)}")
        _sync_images(src_store, dst_store, prod, dst_pid)
        _sync_videos(src_store, dst_store, pid, dst_pid)

        # Cross-link & finalize
        write_cross_links(src_store, dst_store, pid, dst_pid)
        set_mf(src_store["domain"], src_store["token"], pid, "sync", "last_hash",
               "single_line_text_field", compute_hash_with_media(src_store, prod, pid))

        mfid = get_mf_id(dst_store["domain"], dst_store["token"], dst_pid, ORIGIN_NS, ORIGIN_KEY)
        if mfid:
            delete_mf(dst_store["domain"], dst_store["token"], mfid)

        info(f"[{src_store['name']} ➝ {dst_store['name']}] sync complete PID {pid} -> {dst_pid}"
             f"{' (created)' if created_now else ''}")

    except Exception as e:
        error(f"[{src_store['name']} ➝ {dst_store['name']}] {e}")
    finally:
        lock.release()

# =========================================================
# Order-driven inventory sync (two-way, delta-based)
# =========================================================
def mirror_sale_to_other_store(other_store: dict, sku: str, qty: int, other_location_legacy_id: Optional[str]):
    info(f"[orders] {other_store['name']} adjust {sku} -{qty}")
    adjust_inventory_by_sku(other_store["domain"], other_store["token"], sku, -abs(qty), other_location_legacy_id)

def mirror_cancel_to_other_store(other_store: dict, sku: str, qty: int, other_location_legacy_id: Optional[str]):
    info(f"[orders] {other_store['name']} adjust {sku} +{qty}")
    adjust_inventory_by_sku(other_store["domain"], other_store["token"], sku, +abs(qty), other_location_legacy_id)
