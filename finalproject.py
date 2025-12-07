"""
Orchestration script to:
 - create DB
 - fetch crimes (UK Police API)
 - reverse geocode crimes (Positionstack)
 - fetch weather for locations (OpenWeatherMap)
 - process & visualize

Set these environment variables before running:
 - POSITIONSTACK_API_KEY
 - OPENWEATHER_API_KEY

Example run:
    python finalproject.py
"""

import os
from src.db_utils import create_connection, create_tables
from src.fetch_crime import fetch_and_store_crimes
from src.fetch_geocode import geocode_and_store_for_unlinked_crimes
from src.fetch_weather import fetch_and_store_weather_for_locations
from src.process_and_visualize import run_all_visualizations

# Parameters you can change:
MAX_INSERT_PER_RUN = 25  # required by rubric: limit inserts per file run to <= 25
# Example location (latitude, longitude) to pull UK Police crimes for.
# Replace these with coordinates you want to query (UK coords)
EXAMPLE_LAT = 51.509865  # London approximate
EXAMPLE_LON = -0.118092
EXAMPLE_MONTH = "2023-09"  # YYYY-MM, change as needed
WEATHER_DATES = ["2023-09-01", "2023-09-02", "2023-09-03"]  # list of dates to fetch (YYYY-MM-DD)

def main():
    pos_key = os.environ.get("POSITIONSTACK_API_KEY")
    owm_key = os.environ.get("OPENWEATHER_API_KEY")
    if not pos_key:
        print("Warning: POSITIONSTACK_API_KEY not set - reverse geocoding will fail until set.")
    if not owm_key:
        print("Warning: OPENWEATHER_API_KEY not set - weather fetching will fail until set.")

    conn = create_connection()
    create_tables(conn)

    # 1) Fetch up to MAX_INSERT_PER_RUN crimes for the sample location & month
    print("Fetching crimes...")
    try:
        fetch_and_store_crimes(conn, EXAMPLE_LAT, EXAMPLE_LON, EXAMPLE_MONTH, max_items=MAX_INSERT_PER_RUN)
    except Exception as e:
        print("Error fetching crimes:", e)

    # 2) Reverse geocode up to MAX_INSERT_PER_RUN crimes that are unlinked
    if pos_key:
        print("Reverse geocoding unlinked crimes and storing LocationData...")
        try:
            geocode_and_store_for_unlinked_crimes(conn, pos_key, max_items=MAX_INSERT_PER_RUN)
        except Exception as e:
            print("Error in reverse geocoding:", e)

    # 3) Fetch weather for the LocationData rows we have (for chosen dates)
    if owm_key:
        print("Fetching weather for locations and dates...")
        try:
            fetch_and_store_weather_for_locations(conn, WEATHER_DATES, max_items=MAX_INSERT_PER_RUN, openweather_api_key=owm_key)
        except Exception as e:
            print("Error fetching weather:", e)

    # 4) Produce visualizations (reads DB and writes PNG + calc csv)
    print("Generating visualizations...")
    run_all_visualizations()

    print("Done.")


if __name__ == "__main__":
    main()
