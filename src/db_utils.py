import sqlite3
import os

DB_NAME= 'crime_weather.db'

def set_up_database(db_name: str= DB_NAME):
    base_dir=os.path.dirname(os.path.abspath(__file__))
    db_path= os.path.join(base_dir, db_name)
    conn= sqlite3.connect(db_path)
    cur= conn.cursor()
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
                
                
    
            