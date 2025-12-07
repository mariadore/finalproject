import os
from src.db_utils import set_up_database
from src.fetch_crime import fetch_and_store_crimes
from src.fetch_geocode import geocode_and_attach_locations
from src.fetch_weather import fetch_weather_for_all_locations
from src.process_and_visualize import run_visualizations

LAT = 51.509865
LON = -0.118092
MONTH = "2023-09"

DATES = ["2023-09-01", "2023-09-02", "2023-09-03"]

def main():
    conn_path, conn = set_up_database()

    pos_key = os.environ.get("POSITIONSTACK_API_KEY")
    owm_key = os.environ.get("OPENWEATHER_API_KEY")

    print("Fetching UK crimes...")
    fetch_and_store_crimes(conn, LAT, LON, MONTH)

    if pos_key:
        print("Reverse geocoding crime locations...")
        geocode_and_attach_locations(conn, pos_key)
    else:
        print("No POSITIONSTACK_API_KEY provided.")

    if owm_key:
        print("Fetching weather...")
        fetch_weather_for_all_locations(conn, owm_key, DATES)
    else:
        print("No OPENWEATHER_API_KEY provided.")

    print("Generating visualizations...")
    run_visualizations(conn)


if __name__ == "__main__":
    main()
