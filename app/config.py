import os

API_VERSION = os.getenv("API_VERSION", "2025-04")
BASE_URL = os.getenv("BASE_URL")

TAF = {
    "domain": os.getenv("TAF_STORE_DOMAIN"),
    "token": os.getenv("TAF_ACCESS_TOKEN"),
    "secret": os.getenv("TAF_API_SECRET"),
    "location_id": os.getenv("TAF_PRIMARY_LOCATION_ID"),
    "name": "TAF",
}

AFL = {
    "domain": os.getenv("AFL_STORE_DOMAIN"),
    "token": os.getenv("AFL_ACCESS_TOKEN"),
    "secret": os.getenv("AFL_API_SECRET"),
    "location_id": os.getenv("AFL_PRIMARY_LOCATION_ID"),
    "name": "AFL",
}
