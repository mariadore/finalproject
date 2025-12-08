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

    print("Fetching UK crimes...")
    fetch_and_store_crimes(conn, LAT, LON, MONTH)

    print("Reverse geocoding crime locations...")
    geocode_and_attach_locations(conn)

    print("Fetching weather...")
    fetch_weather_for_all_locations(conn, dates=DATES)

    print("Generating visualizations...")
    run_visualizations(conn)


if __name__ == "__main__":
    main()
