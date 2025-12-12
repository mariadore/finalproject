"""
db_utils.py

Utility functions for creating, migrating, and maintaining the SQLite database
used for crime, weather, and transit analysis. Includes insert helpers, schema
migrations, and API cursor tracking utilities.
"""

import calendar
import hashlib
import os
import sqlite3
from typing import Optional, Tuple

DB_NAME = 'crime_weather.db'


def set_up_database(db_name: str = DB_NAME) -> Tuple[str, sqlite3.Connection]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    db_path = os.path.join(data_dir, db_name)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    # CrimeData table
    # Core crime table storing all incidents from UK Police API
    cur.execute("""
        CREATE TABLE IF NOT EXISTS CrimeData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            crime_id TEXT UNIQUE,
            persistent_id TEXT,
            month TEXT,
            category TEXT,
            latitude REAL,
            longitude REAL,
            street_id INTEGER,
            street_name TEXT,
            outcome_category TEXT,
            outcome_date TEXT,
            crime_date TEXT,
            location_id INTEGER
        );
    """)

    # LocationData table
    # Normalized location table for each geocoded area
    cur.execute("""
        CREATE TABLE IF NOT EXISTS LocationData (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            county TEXT,
            region TEXT,
            lat REAL,
            lon REAL,
            label TEXT UNIQUE
        );
    """)

    # StreetData table (normalized street metadata from UK Police API)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS StreetData (
            street_id INTEGER PRIMARY KEY,
            street_name TEXT
        );
    """)

    # WeatherData table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER,
            date TEXT,
            temp_c REAL,
            temp_min_c REAL,
            temp_max_c REAL,
            precip_mm REAL,
            wind_speed REAL,
            humidity INTEGER,
            weather_main TEXT,
            UNIQUE(location_id, date),
            FOREIGN KEY (location_id) REFERENCES LocationData(location_id)
        );
    """)

    # Transit stops table (TfL)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TransitStops (
            stop_id INTEGER PRIMARY KEY AUTOINCREMENT,
            naptan_id TEXT UNIQUE,
            common_name TEXT,
            stop_type TEXT,
            modes TEXT,
            lat REAL,
            lon REAL
        );
    """)

    # API cursor tracking table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_cursors (
            api_name TEXT PRIMARY KEY,
            last_fetched TEXT
        );
    """)

    ensure_crime_date_column(conn)

    # Helpful indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crimedata_loc_month ON CrimeData(location_id, month);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crimedata_loc_date ON CrimeData(location_id, crime_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weatherdata_loc_date ON WeatherData(location_id, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transit_lat_lon ON TransitStops(lat, lon);")

    populate_missing_crime_dates(conn)
    migrate_street_data(conn)

    conn.commit()
    print("Database and tables created successfully.")
    return db_path, conn


# -----------------------
# INSERT / UPDATE HELPERS
# -----------------------

def insert_location(conn, city, county, region, lat, lon, label) -> int:
    """
    Insert or return existing LocationData entry.
    Returns location_id.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO LocationData (city, county, region, lat, lon, label)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (city, county, region, lat, lon, label))
    conn.commit()

    cur.execute("SELECT location_id FROM LocationData WHERE label = ?", (label,))
    row = cur.fetchone()
    return row[0] if row else None


def insert_crime(conn, crime: dict) -> None:
    """
    Insert a normalized UK Police API crime dict into CrimeData.
    """
    street_id = crime.get("street_id")
    street_name = crime.get("street_name")
    if street_id is not None:
        upsert_street(conn, street_id, street_name)

    crime_date = derive_crime_date(
        crime.get("month"),
        crime.get("crime_id") or crime.get("persistent_id") or crime.get("street_id") or crime.get("category")
    )

    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO CrimeData (
            crime_id, persistent_id, month, category,
            latitude, longitude, street_id, street_name,
            outcome_category, outcome_date, crime_date, location_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        crime.get("crime_id"),
        crime.get("persistent_id"),
        crime.get("month"),
        crime.get("category"),
        crime.get("latitude"),
        crime.get("longitude"),
        street_id,
        street_name,
        crime.get("outcome_category"),
        crime.get("outcome_date"),
        crime_date,
        crime.get("location_id")
    ))
    conn.commit()


def derive_crime_date(month_str, seed_value):
    """
    Deterministically assign a date within the given month using a hash-based seed.
    """
    if not month_str:
        return None

    try:
        year, month = map(int, month_str.split("-"))
    except ValueError:
        return None

    days_in_month = calendar.monthrange(year, month)[1]
    seed = str(seed_value or f"{month_str}-seed")
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()
    day = (int(digest[:8], 16) % days_in_month) + 1
    return f"{year:04d}-{month:02d}-{day:02d}"


def insert_weather(conn, weather: dict) -> None:
    """
    Insert weather row into WeatherData. Skips duplicates based on UNIQUE(location_id, date).
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO WeatherData (
            location_id, date, temp_c, temp_min_c, temp_max_c,
            precip_mm, wind_speed, humidity, weather_main
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        weather.get("location_id"),
        weather.get("date"),
        weather.get("temp_c"),
        weather.get("temp_min_c"),
        weather.get("temp_max_c"),
        weather.get("precip_mm"),
        weather.get("wind_speed"),
        weather.get("humidity"),
        weather.get("weather_main")
    ))
    conn.commit()


def insert_transit_stop(conn, stop):
    """
    Insert TfL transit stop metadata.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO TransitStops (
            naptan_id, common_name, stop_type, modes, lat, lon
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        stop.get("naptan_id"),
        stop.get("common_name"),
        stop.get("stop_type"),
        stop.get("modes"),
        stop.get("lat"),
        stop.get("lon")
    ))
    conn.commit()


def get_unlinked_crimes(conn, limit=25) -> list:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, crime_id, latitude, longitude
        FROM CrimeData
        WHERE location_id IS NULL
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        LIMIT ?
    """, (limit,))
    return cur.fetchall()


def link_crime_to_location(conn, crime_pk, location_id):
    """
    Update CrimeData to connect the row's id → a location_id.
    """
    cur = conn.cursor()
    cur.execute("""
        UPDATE CrimeData
        SET location_id = ?
        WHERE id = ?
    """, (location_id, crime_pk))
    conn.commit()


def release_default_location_links(conn, limit=25):
    """
    Reset a limited number of crimes that were tied to the DEFAULT_LONDON location
    back to NULL so fresh geocode attempts can run in future executions.
    """
    cur = conn.cursor()
    cur.execute("SELECT location_id FROM LocationData WHERE label = 'DEFAULT_LONDON'")
    row = cur.fetchone()
    if not row:
        return 0

    default_id = row[0]
    cur.execute("""
        SELECT id FROM CrimeData
        WHERE location_id = ?
        LIMIT ?
    """, (default_id, limit))
    ids = [r[0] for r in cur.fetchall()]
    if not ids:
        return 0

    for crime_pk in ids:
        cur.execute("""
            UPDATE CrimeData
            SET location_id = NULL
            WHERE id = ?
        """, (crime_pk,))

    conn.commit()
    return len(ids)


if __name__ == "__main__":
    db_path, conn = set_up_database()
    conn.close()


def get_all_locations(conn, limit=None) -> list:
    """
    Return all locations with coordinates, optionally limited.
    """
    cur = conn.cursor()
    query = """
        SELECT location_id, lat, lon
        FROM LocationData
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """
    params = ()

    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    cur.execute(query, params)
    return cur.fetchall()


def get_transit_stop_count(conn):
    """
    Return the total number of transit stops currently stored in TransitStops.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM TransitStops;")
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


def ensure_crime_date_column(conn):
    """
    Add crime_date column if running against older schema.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(CrimeData);")
    columns = [row[1] for row in cur.fetchall()]
    if "crime_date" not in columns:
        cur.execute("ALTER TABLE CrimeData ADD COLUMN crime_date TEXT;")
        conn.commit()


def populate_missing_crime_dates(conn):
    """
    Populate crime_date for existing records lacking the value.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT id, month, crime_id, persistent_id, street_id, category
        FROM CrimeData
        WHERE crime_date IS NULL OR crime_date = ''
    """)
    rows = cur.fetchall()
    if not rows:
        return

    for pk, month_str, crime_id, persistent_id, street_id, category in rows:
        crime_date = derive_crime_date(
            month_str,
            crime_id or persistent_id or street_id or category or pk
        )
        cur.execute(
            "UPDATE CrimeData SET crime_date = ? WHERE id = ?",
            (crime_date, pk)
        )

    conn.commit()


def get_api_cursor(conn, api_name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Fetch the last cursor/token stored for an API. Returns default if missing.
    """
    cur = conn.cursor()
    cur.execute("SELECT last_fetched FROM api_cursors WHERE api_name = ?", (api_name,))
    row = cur.fetchone()
    return row[0] if row else default


def set_api_cursor(conn, api_name: str, value: str):
    """
    Upsert the cursor/token for an API into api_cursors.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO api_cursors (api_name, last_fetched)
        VALUES (?, ?)
        ON CONFLICT(api_name) DO UPDATE
        SET last_fetched = excluded.last_fetched
    """, (api_name, value))
    conn.commit()


def upsert_street(conn, street_id, street_name):
    """
    Ensure StreetData stores each street once (UK Police API requirement).
    """
    if street_id is None:
        return

    normalized_name = street_name.strip() if isinstance(street_name, str) else street_name
    if normalized_name == "":
        normalized_name = None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO StreetData (street_id, street_name)
        VALUES (?, ?)
        ON CONFLICT(street_id) DO UPDATE
        SET street_name = COALESCE(excluded.street_name, street_name)
    """, (street_id, normalized_name))
    conn.commit()


def migrate_street_data(conn):
    """
    Backfill StreetData from any legacy records.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO StreetData (street_id, street_name)
        SELECT DISTINCT street_id, street_name
        FROM CrimeData
        WHERE street_id IS NOT NULL
          AND TRIM(COALESCE(street_name, '')) != ''
    """)
    conn.commit()

    cur.execute("""
        UPDATE CrimeData
        SET street_name = NULL
        WHERE TRIM(COALESCE(street_name, '')) != ''
    """)
    conn.commit()
