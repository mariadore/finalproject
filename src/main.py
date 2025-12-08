import os
from src.db_utils import set_up_database
from src.fetch_crime import fetch_and_store_crimes
from src.fetch_geocode import geocode_and_attach_locations
from src.fetch_weather import fetch_weather_for_all_locations
from src.visualize import run_visualizations

# Default location: London, UK
LAT = 51.509865
LON = -0.118092
MONTH = "2023-09"

# Example dates for weather
DATES = ["2023-09-01", "2023-09-02", "2023-09-03"]

def main():
    # Set up database
    db_path, conn = set_up_database()

    # Load API keys from environment variables
    pos_key = os.environ.get("POSITIONSTACK_API_KEY")
    owm_key = os.environ.get("OPENWEATHER_API_KEY")

    # 1. Fetch crimes
    print("Fetching UK crimes...")
    fetch_and_store_crimes(conn, LAT, LON, MONTH)

    # 2. Reverse geocode crimes
    if pos_key:
        print("Reverse geocoding crime locations...")
        geocode_and_attach_locations(conn, pos_key)
    else:
        print("No POSITIONSTACK_API_KEY provided. Skipping geocoding.")

    # 3. Fetch weather
    if owm_key:
        print("Fetching weather for all locations...")
        fetch_weather_for_all_locations(conn, owm_key, DATES)
    else:
        print("No OPENWEATHER_API_KEY provided. Skipping weather fetch.")

    # 4. Run visualizations
    print("Generating visualizations...")
    run_visualizations(conn)

    # Close database connection
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
