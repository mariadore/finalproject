import calendar
import random
from src.db_utils import set_up_database
from src.fetch_crime import fetch_and_store_crimes
from src.fetch_geocode import geocode_and_attach_locations
from src.fetch_weather import fetch_weather_for_all_locations
from src.analysis import (
    calculate_crimes_by_weather,
    calculate_crimes_by_temperature_bins,
    calculate_crime_type_distribution
)
from src.visualize import visualize_results


def expand_month_to_dates(month_str):
    """Convert a crime month (YYYY-MM) → list of YYYY-MM-DD dates."""
    year, month = map(int, month_str.split("-"))
    num_days = calendar.monthrange(year, month)[1]
    return [f"{year}-{month:02d}-{day:02d}" for day in range(1, num_days + 1)]


def get_unique_crime_months(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT month FROM CrimeData;")
    return [row[0] for row in cur.fetchall()]


def main():
    print("Setting up database...")
    db_path, conn = set_up_database()

    # Check existing table counts
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM CrimeData;")
    crime_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM LocationData;")
    loc_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM WeatherData;")
    weather_count = cur.fetchone()[0]

    # Fetch crime data only if needed
    LAT = 51.515
    LON = -0.13
    MONTH = "2023-09"

    if crime_count == 0:
        print("Fetching UK crimes...")
        fetch_and_store_crimes(conn, LAT, LON, MONTH, max_items=50)
    else:
        print(f"CrimeData already populated ({crime_count} rows) → skipping fetch.")

    # Reverse geocode only if needed
    if loc_count == 0:
        print("Reverse geocoding crime locations...")
        geocode_and_attach_locations(conn, max_items=5)
    else:
        print("LocationData already populated → skipping geocoding.")

    # Build weather date list
    print("Preparing weather date list...")
    crime_months = get_unique_crime_months(conn)
    DATES = []

    for m in crime_months:
        all_days = expand_month_to_dates(m)
        DATES.extend(random.sample(all_days, 5))  # random 5 days per month

    # Fetch weather only if needed
    if weather_count == 0:
        print(f"Fetching weather for {len(DATES)} days...")
        fetch_weather_for_all_locations(conn, dates=DATES, max_items=5)
    else:
        print("WeatherData already populated → skipping weather fetch.")

    # Analysis
    print("Computing analysis...")
    df_weather = calculate_crimes_by_weather(conn)
    df_temp = calculate_crimes_by_temperature_bins(conn)
    df_types = calculate_crime_type_distribution(conn)

    # Visualizations
    print("Generating visualizations...")
    visualize_results(df_weather, df_temp, df_types)

    print("Done! Visualizations saved.")


if __name__ == "__main__":
    main()
