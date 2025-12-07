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
