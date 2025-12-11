import requests
import time
from .db_utils import insert_crime

#api 1: Police API
UK_POLICE_BASE = "https://data.police.uk/api"


# POLYGON FOR CENTRAL LONDON (Westminster / Waterloo / Soho)
DEFAULT_LONDON_POLY = (
    "51.520,-0.155:"
    "51.510,-0.155:"
    "51.500,-0.135:"
    "51.495,-0.115:"
    "51.500,-0.095:"
    "51.510,-0.095:"
    "51.520,-0.115"
)


def fetch_crimes_poly(poly, month):
    """
    Fetch UK Police API crimes using polygon instead of lat/lon.
    Returns full crime list for the polygon area.
    """

    url = f"{UK_POLICE_BASE}/crimes-street/all-crime"
    params = {"poly": poly, "date": month}

    try:
        print(f"Fetching crimes for polygon area ({month})…")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    except Exception as e:
        print("⚠ Polygon API failed:", e)
        print("Using fallback sample data instead.")
        with open("data/sample_crimes.json") as f:
            import json
            return json.load(f)


def normalize_crime(raw):
    """Convert UK Police API format → normalized dictionary."""

    loc = raw.get("location") or {}
    street = loc.get("street") or {}
    outcome = raw.get("outcome_status") or {}

    return {
        "crime_id": raw.get("id"),
        "persistent_id": raw.get("persistent_id"),
        "month": raw.get("month"),
        "category": raw.get("category", ""),
        "latitude": float(loc.get("latitude")) if loc.get("latitude") else None,
        "longitude": float(loc.get("longitude")) if loc.get("longitude") else None,
        "street_id": street.get("id"),
        "street_name": street.get("name", ""),
        "outcome_category": outcome.get("category", ""),
        "outcome_date": outcome.get("date"),
        "location_id": None
    }


def fetch_and_store_crimes(conn, month, poly=DEFAULT_LONDON_POLY, max_items=25):
    """
    Fetch crime data from API (polygon) and store in database.
    - Uses polygon to fetch crimes covering a larger London area
    - Stores up to `max_items` crimes
    """

    crimes = fetch_crimes_poly(poly, month)

    if not crimes:
        print("⚠ No crimes received from API.")
        return

    crimes = crimes[:max_items]

    print(f"Inserting {len(crimes)} crime rows into database...")

    for row in crimes:
        crime = normalize_crime(row)
        insert_crime(conn, crime)

    # small delay for safety
    time.sleep(1)
    print("Crime data inserted successfully.")
