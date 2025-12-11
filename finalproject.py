import calendar
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
        fetch_and_store_crimes(conn, MONTH, max_items=500)
    else:
        print(f"CrimeData already populated ({crime_count} rows) → skipping fetch.")

    # Reverse geocode only if needed
    if loc_count == 0:
        print("Reverse geocoding crime locations...")
        geocode_and_attach_locations(conn, max_items=10)
    else:
        print("LocationData already populated → skipping geocoding.")

    # Build weather date list
    print("Preparing weather date list...")
    crime_months = get_unique_crime_months(conn)
    DATES = []

    for m in crime_months:
        DATES.extend(expand_month_to_dates(m))

    DATES = sorted(set(DATES))

    # Fetch weather only if needed
    if weather_count == 0:
        print(f"Fetching weather for {len(DATES)} days across {len(crime_months)} months...")
    else:
        print("Refreshing weather data to ensure all dates/locations covered...")
    fetch_weather_for_all_locations(conn, dates=DATES)

    # Analysis
    print("Computing analysis...")
    df_weather = calculate_crimes_by_weather(conn)
    df_temp = calculate_crimes_by_temperature_bins(conn)
    df_types = calculate_crime_type_distribution(conn)
    df_wind = calculate_crimes_vs_wind(conn)
    df_rain = calculate_precipitation_effect(conn)

    # Visualizations
    print("Generating visualizations...")
    visualize_results(df_weather, df_temp, df_types, df_wind, df_rain)

    print("Done! Visualizations saved.")


if __name__ == "__main__":
    main()
