# app/services/sync.py
import json
import time
import threading
from typing import Optional, Tuple, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..clients.shopify import admin_base, rest_headers, graphql
from ..utils.logger import debug, info, warn, error

# =========================================================
# Locks, Debounce & Mute
# =========================================================

_locks: dict[str, threading.Lock] = {}
_RECENT: dict[str, float] = {}       # per (store:pid) debounce
_MUTE_UNTIL: dict[str, float] = {}   # per (store:pid) mute after we write to DST

DEBOUNCE_SEC = 25   # collapse Shopify bursty emits
MUTE_SEC = 25       # ignore echoes for a short TTL after our own writes

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
# Utilities
# =========================================================

ORIGIN_NS = "sync"
ORIGIN_KEY = "origin"

def has_fireplace_tag(prod: dict) -> bool:
    return "fireplace" in (prod.get("tags", "") or "").lower()

def _norm_images(prod: dict) -> List[str]:
    return [img.get("src") for img in (prod.get("images") or []) if img.get("src")][:20]

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

# =========================================================
# GraphQL helpers
# =========================================================

FIND_BY_SKU = """
query($q:String!) {
  productVariants(first: 10, query: $q) {
    edges {
      node {
        id
        sku
        product { id title status tags handle }
      }
    }
  }
}
"""

PRODUCT_BY_HANDLE = """
query($handle:String!) {
  productByHandle(handle:$handle) {
    id
    title
    handle
  }
}
"""

PRODUCT_CREATE_MEDIA = """
mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
  productCreateMedia(productId: $productId, media: $media) {
    media {
      alt
      mediaContentType
      status
    }
    mediaUserErrors {
      field
      message
    }
  }
}
"""

def find_variant_by_sku(domain: str, token: str, sku: str):
    data = graphql(domain, token, FIND_BY_SKU, {"q": f"sku:{sku}"})
    edges = data.get("data", {}).get("productVariants", {}).get("edges", [])
    if not edges:
        return None, None
    node = edges[0]["node"]
    return node["product"]["id"], node["id"]

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
        "namespace": ns, "key": key, "type": type_,
        "value": value, "owner_resource": "product", "owner_id": pid
    }}
    requests.post(f"{admin_base(domain)}/metafields.json",
                  headers=rest_headers(token), json=payload, timeout=25)

def delete_mf(domain: str, token: str, mf_id: int):
    requests.delete(f"{admin_base(domain)}/metafields/{mf_id}.json",
                    headers=rest_headers(token), timeout=20)

def delete_product(domain: str, token: str, pid: str):
    requests.delete(f"{admin_base(domain)}/products/{pid}.json",
                    headers=rest_headers(token), timeout=25)

def list_images(domain: str, token: str, pid: str | int) -> list[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}/images.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("images", [])
    return []

def delete_all_images(domain: str, token: str, pid: str | int):
    images = list_images(domain, token, pid)
    for img in images:
        iid = img.get("id")
        if not iid:
            continue
        requests.delete(f"{admin_base(domain)}/products/{pid}/images/{iid}.json",
                        headers=rest_headers(token), timeout=25)

def add_image(domain: str, token: str, pid: str | int, src: str, position: int, alt: Optional[str] = None):
    payload = {"image": {"src": src, "position": position}}
    if alt:
        payload["image"]["alt"] = alt
    r = requests.post(f"{admin_base(domain)}/products/{pid}/images.json",
                      headers=rest_headers(token), json=payload, timeout=60)
    if r.status_code not in (200, 201):
        warn(f"[media] add_image failed on {domain} pid={pid} pos={position}: {r.status_code} {r.text}")

def list_media(domain: str, token: str, pid: str | int) -> list[dict]:
    r = requests.get(f"{admin_base(domain)}/products/{pid}/media.json",
                     headers=rest_headers(token), timeout=25)
    if r.status_code == 200:
        return r.json().get("media", [])
    return []

def delete_all_media(domain: str, token: str, pid: str | int):
    media = list_media(domain, token, pid)
    for m in media:
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
# Cross-link helpers (idempotency / no-dupes)
# ---------------------------------------------------------
# We store each store’s counterpart product ID in metafields:
#   - On TAF product:   sync.afl_product_id = <AFL numeric id>
#   - On AFL product:   sync.taf_product_id = <TAF numeric id>
#
# get_cross_link_pid_on_dst(...)  -> reads source’s cross-link to quickly resolve the
#                                    destination product id (prefer this over handle/SKU).
# write_cross_links(...)          -> writes both directions after a successful sync so
#                                    future events are deterministic and creation is
#                                    never attempted twice.
# =========================================================

def get_cross_link_pid_on_dst(src_store: dict, dst_store: dict, src_pid: str | int) -> Optional[str]:
    """Return destination product id from the source’s cross-link metafield, if present."""
    key = "afl_product_id" if dst_store["name"] == "AFL" else "taf_product_id"
    val = get_mf(src_store["domain"], src_store["token"], src_pid, "sync", key)
    if not val:
        return None
    # Normalize gid -> numeric id
    if isinstance(val, str):
        if val.isdigit():
            return val
        if "gid://shopify/Product/" in val:
            return val.split("/")[-1]
    return str(val)

def write_cross_links(src_store: dict, dst_store: dict, src_pid: str | int, dst_pid: str | int):
    """Write cross-link metafields on both products (source & destination)."""
    # On source, save destination id
    set_mf(
        src_store["domain"], src_store["token"], src_pid,
        "sync",
        "afl_product_id" if dst_store["name"] == "AFL" else "taf_product_id",
        "single_line_text_field",
        str(dst_pid),
    )
    # On destination, save source id
    set_mf(
        dst_store["domain"], dst_store["token"], dst_pid,
        "sync",
        "taf_product_id" if src_store["name"] == "TAF" else "afl_product_id",
        "single_line_text_field",
        str(src_pid),
    )

# =========================================================
# Retry wrappers for product PUT/POST
# =========================================================

class TransientWriteError(Exception): pass

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
# Inventory helpers (real availability)
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

def ensure_tracking_enabled_on_product(domain: str, token: str, pid: str | int):
    """Enable inventory tracking for all variants on destination product if missing."""
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
    if qty is None:
        return
    prod_gid, var_gid = find_variant_by_sku(domain, token, sku)
    if not var_gid:
        return
    variant_id = var_gid.split("/")[-1]
    r = requests.get(f"{admin_base(domain)}/variants/{variant_id}.json",
                     headers=rest_headers(token), timeout=20)
    if r.status_code != 200:
        return
    inventory_item_id = (r.json().get("variant") or {}).get("inventory_item_id")
    if not inventory_item_id:
        return
    if not location_id_legacy:
        location_id_legacy = _primary_location_id(domain, token)
    if not location_id_legacy:
        return
    _connect_level_if_needed(domain, token, inventory_item_id, location_id_legacy)
    payload = {"location_id": int(location_id_legacy), "inventory_item_id": int(inventory_item_id), "available": int(qty)}
    rr = requests.post(f"{admin_base(domain)}/inventory_levels/set.json",
                       headers=rest_headers(token), json=payload, timeout=20)
    if rr.status_code not in (200, 201):
        warn(f"[inventory] set failed for {sku} on {domain}: {rr.status_code} {rr.text}")

def adjust_inventory_by_sku(domain: str, token: str, sku: str, delta: int, location_id_legacy: Optional[str]):
    if not sku or not delta:
        return
    prod_gid, var_gid = find_variant_by_sku(domain, token, sku)
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
    adj = {"location_id": int(location_id_legacy), "inventory_item_id": int(inventory_item_id), "available_adjustment": int(delta)}
    rr = requests.post(f"{admin_base(domain)}/inventory_levels/adjust.json",
                       headers=rest_headers(token), json=adj, timeout=20)
    if rr.status_code not in (200, 201):
        warn(f"[inventory] Adjust failed for {sku} on {domain}: {rr.status_code} {rr.text}")

def mirror_inventory_values_from_src_to_dst(src_store: dict, dst_store: dict, src_prod: dict, src_location_legacy: Optional[str], dst_location_legacy: Optional[str]):
    # Ensure tracking enabled on destination product before setting absolute quantities
    dst_pid = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], [v.get("sku") for v in (src_prod.get("variants") or []) if v.get("sku")])
    if dst_pid:
        ensure_tracking_enabled_on_product(dst_store["domain"], dst_store["token"], dst_pid)

    for v in src_prod.get("variants", []):
        sku = v.get("sku")
        if not sku:
            continue
        prod_gid, var_gid = find_variant_by_sku(src_store["domain"], src_store["token"], sku)
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
# Media sync helpers (one-way: TAF -> AFL only)
# =========================================================

def _sync_images(src_store: dict, dst_store: dict, src_prod: dict, dst_pid: str | int):
    if src_store["name"] != "TAF":
        return  # do not sync media from AFL -> TAF
    src_images = _norm_images(src_prod)
    info(f"[media] syncing {len(src_images)} images {src_store['name']} -> {dst_store['name']} (dst pid {dst_pid})")
    try:
        delete_all_images(dst_store["domain"], dst_store["token"], dst_pid)
        for idx, src in enumerate(src_images, start=1):
            add_image(dst_store["domain"], dst_store["token"], dst_pid, src, idx)
    except Exception as e:
        warn(f"[media] image sync error: {e}")

def _get_media_urls(domain: str, token: str, pid: int | str) -> Tuple[List[str], List[str]]:
    """
    Returns (hosted_video_urls, external_embed_urls)
    """
    try:
        r = requests.get(f"{admin_base(domain)}/products/{pid}/media.json",
                         headers=rest_headers(token), timeout=25)
        if r.status_code != 200:
            return [], []
        items = r.json().get("media", []) or []
        hosted, external = [], []
        for m in items:
            t = (m.get("media_type") or m.get("type") or "").lower()
            if t == "video":
                for s in (m.get("sources") or []):
                    url = s.get("url")
                    if url and url.lower().endswith((".mp4", ".mov", ".m4v")):
                        hosted.append(url)
                        break
            elif t in ("external_video", "external-video"):
                ev = m.get("external_video") or {}
                if ev.get("embed_url"):
                    external.append(ev["embed_url"])
        return hosted, external
    except Exception:
        return [], []

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
    # Only mirror TAF -> AFL per spec
    if src_store["name"] != "TAF":
        return
    try:
        hosted, external = _get_media_urls(src_store["domain"], src_store["token"], src_pid)
        if not hosted and not external:
            return

        info(f"[media] syncing videos (hosted:{len(hosted)} external:{len(external)}) {src_store['name']} -> {dst_store['name']} (dst pid {dst_pid})")

        # wipe destination media first
        delete_all_media(dst_store["domain"], dst_store["token"], dst_pid)

        # hosted videos via GraphQL
        if hosted:
            _product_create_media_videos(dst_store["domain"], dst_store["token"], dst_pid, hosted)

        # external videos via REST
        for u in external[:10]:
            create_media_external_video(dst_store["domain"], dst_store["token"], dst_pid, u)

    except Exception as e:
        warn(f"[media] video sync error: {e}")

# =========================================================
# Core sync handler
# =========================================================

def _delete_on_dst_by_skus(dst_store: dict, skus: list[str]):
    if not skus:
        return
    dst_pid = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], skus)
    if dst_pid:
        info(f"[delete] removing PID {dst_pid} from {dst_store['name']}")
        delete_product(dst_store["domain"], dst_store["token"], dst_pid)
        

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

    try:
        prod = get_product(src_store["domain"], src_store["token"], pid)

        # Deleted case: remove on dst by SKUs
        if not prod:
            skus = [v.get("sku") for v in payload.get("variants", []) if v.get("sku")]
            info(f"[{src_store['name']} ➝ {dst_store['name']}] source deleted, removing on dst (SKUs {skus})")
            _delete_on_dst_by_skus(dst_store, skus)
            return

        # Ignore our own echoes
        origin = get_mf(src_store["domain"], src_store["token"], pid, ORIGIN_NS, ORIGIN_KEY)
        if origin and origin == dst_store["name"]:
            debug(f"[{src_store['name']} ➝ {dst_store['name']}] ignoring event (origin={origin}) pid={pid}")
            return

        # Filter: only 'fireplace' products participate; otherwise delete on dst
        if not has_fireplace_tag(prod):
            info(f"[{src_store['name']}] PID {pid} no fireplace tag, removing from {dst_store['name']}")
            _delete_on_dst_by_skus(dst_store, [v.get("sku") for v in prod.get("variants", []) if v.get("sku")])
            return

        # Hard availability rule from TAF: if draft/archived or total <= 0 => ensure absent on AFL
        if src_store["name"] == "TAF":
            total_avail = product_total_available(src_store["domain"], src_store["token"], prod, src_store.get("location_id"))
            if prod.get("status") in ("draft", "archived") or total_avail <= 0:
                info(f"[TAF] PID {pid} unavailable (status={prod.get('status')}, avail={total_avail}), removing from AFL")
                _delete_on_dst_by_skus(dst_store, [v.get("sku") for v in prod.get("variants", []) if v.get("sku")])
                return

        # Idempotency: skip if content hash matches
        new_hash = compute_hash(prod)
        last_hash = get_mf(src_store["domain"], src_store["token"], pid, "sync", "last_hash")
        if last_hash and last_hash == new_hash:
            debug(f"[{src_store['name']} ➝ {dst_store['name']}] no changes (hash match), skip PID {pid}")
            return

        # Resolve destination product id (strict order):
        # 1) cross-link metafield
        dst_pid = get_cross_link_pid_on_dst(src_store, dst_store, pid)

        # 2) handle exact match
        if not dst_pid and prod.get("handle"):
            dst_pid = find_product_id_by_handle(dst_store["domain"], dst_store["token"], prod.get("handle"))

        # 3) any matching SKU
        src_skus = [v.get("sku") for v in (prod.get("variants") or []) if v.get("sku")]
        if not dst_pid and src_skus:
            dst_pid = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], src_skus)

        # Build base payload (fields safe to PUT/POST)
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

        if dst_pid:
            # UPDATE PATH
            info(f"[{src_store['name']} ➝ {dst_store['name']}] updating PID {dst_pid}")
            product_payload["product"]["id"] = int(dst_pid)
            # mark origin on DST so DST webhook echoes can be muted
            set_mf(dst_store["domain"], dst_store["token"], dst_pid, ORIGIN_NS, ORIGIN_KEY,
                   "single_line_text_field", src_store["name"])
            _put_product_with_retry(dst_store["domain"], dst_store["token"], product_payload)
            _mute_for(_key(dst_store["name"], dst_pid), MUTE_SEC)

        else:
            # CREATE PATH — ONLY ALLOW WHEN SOURCE IS TAF
            if src_store["name"] != "TAF":
                info(f"[{src_store['name']} ➝ {dst_store['name']}] no match; creation disabled for {src_store['name']}. Skip.")
                return

            # preflight (defensive): re-check existence immediately before create
            pre_dst = None
            if prod.get("handle"):
                pre_dst = find_product_id_by_handle(dst_store["domain"], dst_store["token"], prod.get("handle"))
            if not pre_dst and src_skus:
                pre_dst = find_product_id_by_any_sku(dst_store["domain"], dst_store["token"], src_skus)
            if pre_dst:
                dst_pid = pre_dst
                info(f"[{src_store['name']} ➝ {dst_store['name']}] switched to update (found preflight PID {dst_pid})")
                product_payload["product"]["id"] = int(dst_pid)
                set_mf(dst_store["domain"], dst_store["token"], dst_pid, ORIGIN_NS, ORIGIN_KEY,
                       "single_line_text_field", src_store["name"])
                _put_product_with_retry(dst_store["domain"], dst_store["token"], product_payload)
                _mute_for(_key(dst_store["name"], dst_pid), MUTE_SEC)
            else:
                # CREATE
                info(f"[{src_store['name']} ➝ {dst_store['name']}] creating new product")
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
                    "inventory_management": "shopify",
                } for v in (prod.get("variants") or [])]

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
                            set_mf(dst_store["domain"], dst_store["token"], dst_pid, ORIGIN_NS, ORIGIN_KEY,
                                   "single_line_text_field", src_store["name"])
                            _put_product_with_retry(dst_store["domain"], dst_store["token"], product_payload)
                            _mute_for(_key(dst_store["name"], dst_pid), MUTE_SEC)
                        else:
                            raise
                    else:
                        raise

        if not dst_pid:
            warn(f"[{src_store['name']} ➝ {dst_store['name']}] no dst PID after create/update, abort media/inventory")
            return

        # Media: only TAF -> AFL
        _sync_images(src_store, dst_store, prod, dst_pid)
        _sync_videos(src_store, dst_store, pid, dst_pid)

        # Inventory: absolute mirror only when src is TAF
        if src_store["name"] == "TAF":
            mirror_inventory_values_from_src_to_dst(
                src_store, dst_store, prod,
                src_store.get("location_id"), dst_store.get("location_id")
            )

        # Cross-link both sides and finalize
        write_cross_links(src_store, dst_store, pid, dst_pid)
        set_mf(src_store["domain"], src_store["token"], pid, "sync", "last_hash", "single_line_text_field", new_hash)

        # clear origin marker on dst so future legit edits propagate
        mfid = get_mf_id(dst_store["domain"], dst_store["token"], dst_pid, ORIGIN_NS, ORIGIN_KEY)
        if mfid:
            delete_mf(dst_store["domain"], dst_store["token"], mfid)

        info(f"[{src_store['name']} ➝ {dst_store['name']}] sync complete PID {pid} -> {dst_pid}{' (created)' if created_now else ''}")

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
