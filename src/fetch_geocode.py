import requests
import time
from db_utils import insert_location, get_unlinked_crimes, link_crime_to_location

POSITIONSTACK_URL = "http://api.positionstack.com/v1/reverse"


def reverse_geocode(lat, lon, api_key):
    """Call Positionstack reverse geocoding."""
    params = {
        "access_key": api_key,
        "query": f"{lat},{lon}",
        "limit": 1
    }

    resp = requests.get(POSITIONSTACK_URL, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json().get("data") or []
    if not data:
        return None

    d = data[0]

    return {
        "city": d.get("locality") or d.get("county"),
        "county": d.get("county"),
        "region": d.get("region"),
        "lat": lat,
        "lon": lon,
        "label": d.get("label")
    }


def geocode_and_attach_locations(conn, api_key, max_items=25):
    """
    Finds CrimeData rows with location_id=NULL,
    reverse geocodes them, inserts LocationData,
    and updates CrimeData.location_id.
    """
    crimes = get_unlinked_crimes(conn, max_items)

    for crime_id, crime_uid, lat, lon in crimes:
        geo = reverse_geocode(lat, lon, api_key)
        if geo:
            location_id = insert_location(
                conn,
                geo["city"],
                geo["county"],
                geo["region"],
                geo["lat"],
                geo["lon"],
                geo["label"]
            )
            link_crime_to_location(conn, crime_id, location_id)

        time.sleep(0.5)  # API-friendly
