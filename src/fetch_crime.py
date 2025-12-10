import requests
import time
import json
import os
from .db_utils import insert_crime

UK_POLICE_BASE = "https://data.police.uk/api"


def fetch_crimes(lat, lon, month):
    url = f"{UK_POLICE_BASE}/crimes-street/all-crime"
    params = {"lat": lat, "lng": lon, "date": month}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("⚠️ Live API failed, using local sample data instead.")
        import json
        with open("data/sample_crimes.json") as f:
            return json.load(f)


def normalize_crime(raw):
    """Convert UK Police API format → database-ready dict."""
    loc = raw.get("location") or {}
    street = loc.get("street") or {}
    outcome = raw.get("outcome_status") or {}

    return {
        "crime_id": raw.get("id"),
        "persistent_id": raw.get("persistent_id"),
        "month": raw.get("month"),
        "category": raw.get("category"),
        "latitude": float(loc.get("latitude")) if loc.get("latitude") else None,
        "longitude": float(loc.get("longitude")) if loc.get("longitude") else None,
        "street_id": street.get("id"),
        "street_name": street.get("name"),
        "outcome_category": outcome.get("category"),
        "outcome_date": outcome.get("date"),
        "location_id": None
    }


def fetch_and_store_crimes(conn, lat, lon, month, max_items=25):
    """
    Fetch crimes & insert up to max_items into CrimeData table.
    """
    crimes = fetch_crimes(lat, lon, month)
    crimes = crimes[:max_items]

    for raw in crimes:
        crime = normalize_crime(raw)
        insert_crime(conn, crime)

    time.sleep(1)
