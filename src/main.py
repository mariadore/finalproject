import os
import pandas as pd

from src.db_utils import set_up_database
from src.fetch_crime import fetch_and_store_crimes
from src.fetch_geocode import geocode_and_attach_locations
from src.fetch_weather import fetch_weather_for_all_locations

from src.analysis import (
    calculate_crimes_by_weather,
    calculate_crimes_by_temperature_bins,
    calculate_crime_type_distribution,
    calculate_crimes_vs_wind,
    calculate_precipitation_effect
)

from src.visualize import visualize_results


MONTH = "2023-09"
DATES = ["2023-09-01", "2023-09-02", "2023-09-03"]


def main():
    #1. Database setup
    db_path, conn = set_up_database()

    # -------------------------------
    # 2. FETCH CRIME DATA
    # -------------------------------
    print("Fetching UK crime data...")
    fetch_and_store_crimes(conn, MONTH)

    # 3. GEOCODE LOCATIONS

    print("Skipping geocoding (disabled for reliability).")
    # geocode_and_attach_locations(conn, pos_key)


    # 4. FETCH WEATHER DATA
 
    # Only run if we have locations (avoids errors)
    has_locations = pd.read_sql_query("SELECT COUNT(*) AS n FROM LocationData;", conn).iloc[0]["n"]

    if has_locations > 0:
        print("Fetching historical weather (TomTom)...")
        fetch_weather_for_all_locations(conn, DATES, max_new_records=25)
    else:
        print("No locations available — skipping weather.")

    # -------------------------------
    # 5. RUN CALCULATIONS
    # -------------------------------
    print("Running analysis queries...")

    df_weather_types = calculate_crimes_by_weather(conn)
    df_temp_bins = calculate_crimes_by_temperature_bins(conn)
    df_type_dist = calculate_crime_type_distribution(conn)
    df_wind = calculate_crimes_vs_wind(conn)
    df_precip = calculate_precipitation_effect(conn)

    print("Analysis DataFrames created successfully.")

    # -------------------------------
    # 6. VISUALIZE
    # -------------------------------
    print("Generating visualizations...")

    visualize_results(
        df_weather_types,
        df_temp_bins,
        df_type_dist,
        df_wind,
        df_precip
    )

    conn.close()
    print("Project complete.")


if __name__ == "__main__":
    main()
