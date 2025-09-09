# app/routes/setup_metafields.py
from flask import Blueprint
from ..config import TAF, AFL
from ..clients.shopify import graphql

bp = Blueprint("setup_metafields", __name__)

# (name, namespace, key, type, description)
DEFS = [
    ("Sync: Last Hash",      "sync", "last_hash",      "single_line_text_field", "Deduplicates no-op updates"),
    ("Sync: TAF Product ID", "sync", "taf_product_id", "number_integer",         "Cross-link to TAF product ID"),
    ("Sync: AFL Product ID", "sync", "afl_product_id", "number_integer",         "Cross-link to AFL product ID"),
]

MUTATION = """
mutation metafieldDefinitionCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition {
      id
      name
      namespace
      key
      type { name category }
      ownerType
    }
    userErrors { field message }
  }
}
"""

def _create_defs(store):
    out = []
    for name, ns, key, type_, desc in DEFS:
        variables = {
            "definition": {
                "name": name,
                "namespace": ns,
                "key": key,
                "type": type_,
                "description": desc,
                "ownerType": "PRODUCT"
            }
        }
        try:
            resp = graphql(store["domain"], store["token"], MUTATION, variables)
        except Exception as e:
            out.append(f"{store['name']} {ns}.{key}: EXC {e}")
            continue

        # Top-level GraphQL errors?
        top_errors = (resp or {}).get("errors")
        if top_errors:
            out.append(f"{store['name']} {ns}.{key}: ERR {top_errors}")
            continue

        data = (resp or {}).get("data", {})
        block = (data.get("metafieldDefinitionCreate") or {})
        created = block.get("createdDefinition")
        errs = block.get("userErrors") or []

        if created:
            out.append(f"{store['name']} {ns}.{key}: OK {created.get('id')}")
            continue

        if errs:
            msg = "; ".join([e.get("message", "") for e in errs])
            # Treat duplicates as success
            if "already been taken" in msg.lower() or "already exists" in msg.lower():
                out.append(f"{store['name']} {ns}.{key}: EXISTS")
            else:
                out.append(f"{store['name']} {ns}.{key}: ERR {msg}")
        else:
            out.append(f"{store['name']} {ns}.{key}: UNKNOWN {resp}")

    return " ; ".join(out)

@bp.get("/create")
def create_defs():
    taf = _create_defs(TAF)
    afl = _create_defs(AFL)
    return f"{taf} | {afl}", 200
