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
    crimes = get_unlinked_crimes(conn, limit=max_items)

    print(f"Attempting TomTom geocoding for {len(crimes)} crimes...")

    for crime_pk, crime_uid, lat, lon in crimes:
        geo = reverse_geocode(lat, lon)
        if geo:
            location_id = insert_location(
                conn, geo["city"], geo["county"], geo["region"],
                geo["lat"], geo["lon"], geo["label"]
            )
            link_crime_to_location(conn, crime_pk, location_id)

        time.sleep(0.3)

    assign_default_location(conn)
    print("TomTom geocoding complete.")


def assign_default_location(conn, lat=51.509865, lon=-0.118092):
    """
    Assigns a single synthetic 'London' location to all crimes missing location_id.
    """
    cur = conn.cursor()

    # Make sure a default location exists
    cur.execute("""
        INSERT OR IGNORE INTO LocationData (city, county, region, lat, lon, label)
        VALUES ('London', 'Greater London', 'London', ?, ?, 'Default London')
    """, (lat, lon))
    conn.commit()

    # Get the id of this location
    cur.execute("SELECT location_id FROM LocationData WHERE label='Default London'")
    default_loc = cur.fetchone()[0]

    # Assign all crimes that still have NULL location_id
    cur.execute("""
        UPDATE CrimeData
        SET location_id = ?
        WHERE location_id IS NULL
    """, (default_loc,))
    conn.commit()

    print("Assigned default location to all un-geocoded crimes.")
