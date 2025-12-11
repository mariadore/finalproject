import requests
import time
from .db_utils import insert_location, get_unlinked_crimes, link_crime_to_location

TOMTOM_API_KEY = "tfFYIYGPk25q0LqtkS2chNGRsdPWLA2y"
TOMTOM_URL = "https://api.tomtom.com/search/2/reverseGeocode/{},{}.json"


def reverse_geocode(lat, lon, api_key=TOMTOM_API_KEY):
    url = TOMTOM_URL.format(lat, lon)
    params = {"key": api_key}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("addresses"):
        return None

    addr = data["addresses"][0]["address"]

    return {
        "city": addr.get("municipality"),
        "county": addr.get("countrySecondarySubdivision"),
        "region": addr.get("countrySubdivision"),
        "lat": lat,
        "lon": lon,
        "label": addr.get("freeformAddress")
    }


def assign_default_location(conn):
    """
    Assign a single synthetic London location to ALL crimes lacking location_id.
    Ensures 500 crimes all get linked to weather.
    """
    cur = conn.cursor()

    # Create default London location
    cur.execute("""
        INSERT OR IGNORE INTO LocationData (city, county, region, lat, lon, label)
        VALUES ('London', 'Greater London', 'London', 51.509865, -0.118092, 'DEFAULT_LONDON')
    """)
    conn.commit()

    # Get ID
    cur.execute("SELECT location_id FROM LocationData WHERE label='DEFAULT_LONDON'")
    default_id = cur.fetchone()[0]

    # Update crimes
    cur.execute("""
        UPDATE CrimeData
        SET location_id = ?
        WHERE location_id IS NULL
    """, (default_id,))
    conn.commit()

    print("Assigned default location to all un-geocoded crimes.")


def geocode_and_attach_locations(conn, max_items=25):
    crimes = get_unlinked_crimes(conn, limit=max_items)

    print(f"TomTom geocoding {len(crimes)} crimes...")

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

        time.sleep(0.25)

    # ALWAYS fill the rest
    assign_default_location(conn)

    print("TomTom geocoding complete.")
