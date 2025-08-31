import requests
from ..config import API_VERSION

def admin_base(domain: str) -> str:
    return f"https://{domain}/admin/api/{API_VERSION}"

def rest_headers(token: str) -> dict:
    return {"Content-Type": "application/json", "X-Shopify-Access-Token": token}

def graphql(domain: str, token: str, query: str, variables=None):
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    r = requests.post(url, headers=rest_headers(token), json={"query": query, "variables": variables or {}}, timeout=30)
    r.raise_for_status()
    return r.json()
