import requests
import time
from .db_utils import insert_location, get_unlinked_crimes, link_crime_to_location

# TomTom Reverse Geocoding endpoint
TOMTOM_API_KEY = "tfFYIYGPk25q0LqtkS2chNGRsdPWLA2y"
TOMTOM_URL = "https://api.tomtom.com/search/2/reverseGeocode/{},{}.json"


def reverse_geocode(lat, lon, api_key=TOMTOM_API_KEY):
    """
    Reverse geocode using TomTom API.
    Returns city, county, region, lat, lon, label.
    """

    url = TOMTOM_URL.format(lat, lon)
    params = {"key": api_key}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # TomTom JSON structure
    if not data.get("addresses"):
        return None

    addr = data["addresses"][0]["address"]

    city = addr.get("municipality") or addr.get("localName")
    county = addr.get("countrySecondarySubdivision")
    region = addr.get("countrySubdivision")
    label = addr.get("freeformAddress")

    return {
        "city": city,
        "county": county,
        "region": region,
        "lat": lat,
        "lon": lon,
        "label": label
    }


def geocode_and_attach_locations(conn, max_items=25):
    """
    Gets crimes with no location_id,
    reverse geocodes them using TomTom,
    inserts into LocationData,
    updates CrimeData.
    """
    crimes = get_unlinked_crimes(conn, limit=max_items)

    print(f"Geocoding {len(crimes)} crimes using TomTom API...")

    for crime_pk, crime_uid, lat, lon in crimes:
        geo = reverse_geocode(lat, lon)

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
            link_crime_to_location(conn, crime_pk, location_id)

        time.sleep(0.3)  # respectful delay

    print("TomTom geocoding complete.")
