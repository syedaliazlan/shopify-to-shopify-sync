# Shopify ↔ Shopify Sync Service

A lightweight Flask-based app to **synchronise products, media, and inventory between two Shopify stores**.  
Designed to run headless on a small server (e.g., DigitalOcean droplet, Heroku, Render, etc.), this service listens to **Shopify webhooks** and mirrors updates in near real-time between:

- **TAF** → **AFL**
- **AFL** → **TAF**

---

## ✨ Features

- 🔄 **Bi-directional product sync**
  - Creates/updates products across both stores.
  - Keeps titles, descriptions, pricing, options, SKUs in sync.
- 🖼 **Media sync**
  - Images and (optionally) videos are mirrored.
- 📦 **Inventory sync**
  - Tracks and updates stock levels.
- 🧩 **Cross-linked metafields**
  - Each product stores its counterpart’s ID (`sync.taf_product_id`, `sync.afl_product_id`).
  - Ensures idempotency — prevents duplicate products.
- 🛡 **Hash-based change detection**
  - Skips redundant updates when nothing has changed.
- ♻️ **Retry & debouncing**
  - Retries `409` conflicts (`This product is currently being modified`).
  - Debounces multiple webhook calls from the same event.
- ⚙️ **Webhooks auto-registration**
  - Registers required webhooks (`products/create`, `products/update`, `products/delete`, `inventory_levels/update`, `orders/*`).

---

## 📂 Project Structure

```
shopify-to-shopify-sync/
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── register.py          # Registers Shopify webhooks
│   ├── sync.py              # Core sync logic (products, media, inventory)
│   ├── webhooks.py          # Webhook endpoints (TAF/AFL)
│   └── clients/
│       └── shopify.py       # Shopify REST/GraphQL helpers
├── wsgi.py                  # Flask entrypoint
├── requirements.txt
└── README.md
```

---

## ⚡ Setup

### 1. Clone repo & install dependencies
```bash
git clone https://github.com/yourname/shopify-to-shopify-sync.git
cd shopify-to-shopify-sync
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Configure environment
Create `.env` in project root:
```ini
# Flask
FLASK_APP=wsgi.py
FLASK_ENV=development

# Base public URL (ngrok / deployed host)
BASE_URL=https://your-public-url.com

# Shopify Store A (TAF)
TAF_DOMAIN=your-taf-store.myshopify.com
TAF_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxxxx

# Shopify Store B (AFL)
AFL_DOMAIN=your-afl-store.myshopify.com
AFL_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxxxx

# Shopify API
API_VERSION=2025-01
```

### 3. Run locally
```bash
flask run
```
or:
```bash
python wsgi.py
```

Expose via [ngrok](https://ngrok.com/) or [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/) to receive webhooks.

### 4. Register webhooks
Visit:
- `http://localhost:5000/register_webhooks/taf`
- `http://localhost:5000/register_webhooks/afl`

### 5. Create metafields (one-time)
Visit:
```
http://localhost:5000/setup/metafields/create
```
This adds definitions for:
- `sync.last_hash`
- `sync.taf_product_id`
- `sync.afl_product_id`

---

## 🚀 Deployment

- Use a production WSGI server like **gunicorn** or **uwsgi**.
- Run behind **nginx** or similar reverse proxy.
- Persist `.env` secrets via your host (Heroku config vars, Docker secrets, etc.).

Example (gunicorn):
```bash
gunicorn wsgi:app --bind 0.0.0.0:5000 --workers 2
```

---

## 📝 Notes

- Shopify may fire **multiple webhooks per event** (product + variants + inventory).  
  This service debounces and hashes payloads to **avoid duplicate updates**.
- Media sync currently handles images. Video support is experimental and may require GraphQL mutation tweaks.
- Products without `inventory_tracking` may log warnings (422). This is expected if the store doesn’t manage inventory for those SKUs.

---

## 🛠 TODO / Future Improvements

- [ ] Video sync via GraphQL mutations.
- [ ] Optional logging → file / database.
- [ ] Admin dashboard for monitoring sync status.
- [ ] Automated tests for conflict resolution.

---

## 📜 License

MIT © 2025 Ali Azlan