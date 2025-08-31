import json, hashlib

def product_hash(prod: dict) -> str:
    fields = {
        "title": prod.get("title",""),
        "body_html": prod.get("body_html",""),
        "tags": prod.get("tags",""),
        "status": prod.get("status",""),
        "variants": [
            {
                "sku": v.get("sku"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "opt1": v.get("option1"),
                "opt2": v.get("option2"),
                "opt3": v.get("option3"),
            } for v in prod.get("variants",[])
        ],
        "images": [img.get("src") for img in prod.get("images",[])],
    }
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()
