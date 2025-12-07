"""
Database utilities for the Crime Weather Analytics project.

Creates tables:
 - CrimeData (crime_id INTEGER PRIMARY KEY AUTOINCREMENT, api_crime_id TEXT UNIQUE, category TEXT, month TEXT, persistent_id TEXT, street_name TEXT, street_id INTEGER, lat REAL, lon REAL, location_id INTEGER)
 - LocationData (location_id INTEGER PRIMARY KEY AUTOINCREMENT, api_lat REAL, api_lon REAL, locality TEXT, region TEXT, label TEXT)
 - WeatherData  (weather_id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, city TEXT, lat REAL, lon REAL, temp REAL, temp_min REAL, temp_max REAL, precipitation REAL, wind_speed REAL, humidity REAL, weather_main TEXT, UNIQUE(date, city, lat, lon))

Note: CrimeData.location_id references LocationData.location_id (integer FK)
"""

import sqlite3
from typing import Optional, Tuple, List, Dict

DB_PATH = "finalproject.db"  # placed in repository root by default


def create_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    # enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    # LocationData table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS LocationData (
        location_id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_lat REAL,
        api_lon REAL,
        locality TEXT,
        region TEXT,
        label TEXT,
        UNIQUE(api_lat, api_lon)
    );
    """)
    # CrimeData table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS CrimeData (
        crime_id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_crime_id TEXT UNIQUE,
        category TEXT,
        month TEXT,
        persistent_id TEXT,
        street_name TEXT,
        street_id INTEGER,
        lat REAL,
        lon REAL,
        location_id INTEGER,
        FOREIGN KEY(location_id) REFERENCES LocationData(location_id)
    );
    """)
    # WeatherData table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS WeatherData (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        city TEXT,
        lat REAL,
        lon REAL,
        temp REAL,
        temp_min REAL,
        temp_max REAL,
        precipitation REAL,
        wind_speed REAL,
        humidity REAL,
        weather_main TEXT,
        UNIQUE(date, city, lat, lon)
    );
    """)
    conn.commit()


def insert_location(conn: sqlite3.Connection, geo: Dict) -> int:
    """
    Insert or get existing location row.
    geo: dict with keys api_lat, api_lon, locality, region, label
    Returns: location_id
    """
    cur = conn.cursor()
    cur.execute("""
    SELECT location_id FROM LocationData WHERE api_lat = ? AND api_lon = ?
    """, (geo["api_lat"], geo["api_lon"]))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
    INSERT INTO LocationData (api_lat, api_lon, locality, region, label)
    VALUES (?, ?, ?, ?, ?)
    """, (geo["api_lat"], geo["api_lon"], geo.get("locality"), geo.get("region"), geo.get("label")))
    conn.commit()
    return cur.lastrowid


def insert_crime(conn: sqlite3.Connection, crime: Dict, location_id: Optional[int]):
    """
    Insert crime record if not present (unique by api_crime_id).
    crime: dict with api_crime_id, category, month, persistent_id, street_name, street_id, lat, lon
    """
    cur = conn.cursor()
    try:
        cur.execute("""
        INSERT OR IGNORE INTO CrimeData (api_crime_id, category, month, persistent_id, street_name, street_id, lat, lon, location_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            crime.get("api_crime_id"),
            crime.get("category"),
            crime.get("month"),
            crime.get("persistent_id"),
            crime.get("street_name"),
            crime.get("street_id"),
            crime.get("lat"),
            crime.get("lon"),
            location_id
        ))
        conn.commit()
    except sqlite3.IntegrityError:
        # unique constraint or FK issue - ignore duplicates
        conn.rollback()


def insert_weather(conn: sqlite3.Connection, weather: Dict):
    """
    Insert weather record if not present (unique on date, city, lat, lon)
    weather: dict with date, city, lat, lon, temp, temp_min, temp_max, precipitation, wind_speed, humidity, weather_main
    """
    cur = conn.cursor()
    cur.execute("""
    INSERT OR IGNORE INTO WeatherData (date, city, lat, lon, temp, temp_min, temp_max, precipitation, wind_speed, humidity, weather_main)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        weather.get("date"),
        weather.get("city"),
        weather.get("lat"),
        weather.get("lon"),
        weather.get("temp"),
        weather.get("temp_min"),
        weather.get("temp_max"),
        weather.get("precipitation"),
        weather.get("wind_speed"),
        weather.get("humidity"),
        weather.get("weather_main")
    ))
    conn.commit()


def count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def query_join_for_calculations(conn: sqlite3.Connection) -> List[Tuple]:
    """
    Example join:
    Join CrimeData with LocationData and WeatherData on approximate matching by date and locality/city.
    This function returns raw rows for later processing.
    """
    cur = conn.cursor()
    # This is an example - real joins might require normalized locality <-> city matching.
    # For simplicity we join where date = month (month stored as YYYY-MM) and locality==city (case-insensitive)
    cur.execute("""
    SELECT c.api_crime_id, c.category, c.month, l.locality, w.date, w.city, w.temp, w.weather_main
    FROM CrimeData c
    LEFT JOIN LocationData l ON c.location_id = l.location_id
    LEFT JOIN WeatherData w ON w.date = c.month AND LOWER(w.city) = LOWER(l.locality)
    WHERE w.date IS NOT NULL
    """)
    return cur.fetchall()
