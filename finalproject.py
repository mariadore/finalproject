import calendar
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
    """
    Convert a crime month (YYYY-MM) → list of YYYY-MM-DD dates.
    """
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

    # Fetch Crime Data (using lat/lon for London)
    print("Fetching UK crimes...")
    # Westminster coordinates
    LAT = 51.515
    LON = -0.13
    MONTH = "2023-09"

    fetch_and_store_crimes(conn, LAT, LON, MONTH, max_items=50)

    # Reverse Geocode Lat/Lon → City/Region
    print("Reverse geocoding crime locations...")
    geocode_and_attach_locations(conn, max_items=5)

    # Expand crime months
    print("Preparing weather date list...")
    crime_months = get_unique_crime_months(conn)

    DATES = []
    for m in crime_months:
        DATES.extend(expand_month_to_dates(m)[:5])  # first 5 days only

    print(f"Fetching weather for {len(DATES)} days...")

    # Fetch Weather for All Locations/Dates
    print("Fetching weather...")
    fetch_weather_for_all_locations(conn, dates=DATES, max_items=5)

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
