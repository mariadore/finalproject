import sqlite3
import os
from typing import Tuple

DB_NAME= 'crime_weather.db'

def set_up_database(db_name: str= DB_NAME)-> Tuple[str, sqlite3.Connection]:
    base_dir=os.path.dirname(os.path.abspath(__file__))
    data_dir=os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    db_path= os.path.join(data_dir, db_name)
    conn= sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur= conn.cursor()
    
    #create CrimeData table
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
            location_id INTEGER
        )
    """)
    
    #create LocationData table
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

    #create WeatherData table
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
<<<<<<< HEAD
    #table to keep track of cursors
    cur.execute("""
    CREATE TABLE IF NOT EXISTS api_cursors (
        api_name TEXT PRIMARY KEY,
        last_fetched TEXT
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_crimedata_loc_date ON CrimeData(location_id, month);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weatherdata_loc_date ON WeatherData(location_id, date);")
    conn.commit()
    print("Database and tables created successfully.")
    return db_path, conn
if __name__ == "__main__":
    db_path, conn = set_up_database()
    conn.close()



                
                
    
            
=======

    # -------------------------
# INSERT HELPERS (MATCH SCHEMA EXACTLY)
# -------------------------

def insert_location(conn, city, county, region, lat, lon, label):
    """
    Insert a row into LocationData.
    UNIQUE constraint: label
    Returns location_id
    """
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO LocationData (city, county, region, lat, lon, label)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (city, county, region, lat, lon, label))
    conn.commit()

    # Fetch the location_id (existing or newly created)
    cur.execute("SELECT location_id FROM LocationData WHERE label = ?", (label,))
    row = cur.fetchone()
    return row[0] if row else None


def insert_crime(conn, crime):
    """
    Insert crime data into CrimeData.
    Unique constraint: crime_id
    crime is a dict with keys:
        crime_id, persistent_id, month, category,
        latitude, longitude, street_id, street_name,
        outcome_category, outcome_date, location_id
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO CrimeData (
            crime_id, persistent_id, month, category,
            latitude, longitude, street_id, street_name,
            outcome_category, outcome_date, location_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        crime.get("crime_id"),
        crime.get("persistent_id"),
        crime.get("month"),
        crime.get("category"),
        crime.get("latitude"),
        crime.get("longitude"),
        crime.get("street_id"),
        crime.get("street_name"),
        crime.get("outcome_category"),
        crime.get("outcome_date"),
        crime.get("location_id")
    ))
    conn.commit()


def insert_weather(conn, weather):
    """
    Insert WeatherData row.
    UNIQUE(location_id, date)
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


def get_unlinked_crimes(conn, limit=25):
    """
    Return crimes missing a location_id.
    """
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


def link_crime_to_location(conn, crime_id, location_id):
    """
    Update CrimeData to add location_id.
    """
    cur = conn.cursor()
    cur.execute("""
        UPDATE CrimeData
        SET location_id = ?
        WHERE id = ?
    """, (location_id, crime_id))
    conn.commit()

>>>>>>> 4046a4a9db77948f8618ce98495bb76c29982d38
