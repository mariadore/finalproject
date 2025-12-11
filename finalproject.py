import calendar
import os
from src.db_utils import set_up_database, release_default_location_links, get_transit_stop_count
from src.fetch_crime import fetch_and_store_crimes
from src.fetch_geocode import geocode_and_attach_locations, assign_default_location
from src.fetch_weather import fetch_weather_for_all_locations
from src.fetch_transit import fetch_transit_stops
from src.analysis import (
    calculate_crimes_by_weather,
    calculate_crimes_by_temperature_bins,
    calculate_crime_type_distribution,
    calculate_crimes_vs_wind,
    calculate_precipitation_effect,
    calculate_crimes_near_transit
)
from src.report import write_analysis_report
from src.visualize import visualize_results

MIN_CRIME_ROWS = 100
MIN_LOCATION_ROWS = 100
MIN_WEATHER_ROWS = 100
MIN_TRANSIT_ROWS = 100
MAX_API_ITEMS_PER_RUN = 25
REPORT_PATH = os.path.join(os.path.dirname(__file__), "analysis_summary.txt")

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

    cur.execute("SELECT COUNT(*) FROM LocationData WHERE label != 'DEFAULT_LONDON';")
    loc_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM WeatherData;")
    weather_count = cur.fetchone()[0]

    transit_count = get_transit_stop_count(conn)

    # Fetch crime data only if needed
    LAT = 51.515
    LON = -0.13
    MONTH = "2023-09"

    if crime_count < MIN_CRIME_ROWS:
        remaining = MIN_CRIME_ROWS - crime_count
        fetch_limit = min(MAX_API_ITEMS_PER_RUN, remaining)
        print(f"Fetching up to {fetch_limit} crimes (need {remaining} more to hit {MIN_CRIME_ROWS}).")
        fetch_and_store_crimes(conn, MONTH, max_items=fetch_limit)
        print("Re-run the script as needed to accumulate at least 100 crimes.")
    else:
        print(f"CrimeData already satisfies minimum rows ({crime_count} rows).")

    # Reverse geocode only if needed
    if loc_count < MIN_LOCATION_ROWS:
        remaining = MIN_LOCATION_ROWS - loc_count
        per_run = min(MAX_API_ITEMS_PER_RUN, remaining)
        released = release_default_location_links(conn, limit=per_run)
        if released:
            print(f"Released {released} previously defaulted crimes for re-geocoding.")
        print(f"Reverse geocoding up to {per_run} crimes (need {remaining} more locations).")
        geocode_and_attach_locations(conn, max_items=per_run)
        cur.execute("SELECT COUNT(*) FROM LocationData WHERE label != 'DEFAULT_LONDON';")
        loc_count = cur.fetchone()[0]
        if loc_count >= MIN_LOCATION_ROWS:
            print("Location minimum met. Assigning default location to remaining crimes.")
            assign_default_location(conn)
        else:
            print("Re-run the script to continue building LocationData via TomTom.")
    else:
        print("LocationData already satisfies the minimum rows.")
        assign_default_location(conn)

    # Transit stops (TfL)
    if transit_count < MIN_TRANSIT_ROWS:
        remaining_transit = MIN_TRANSIT_ROWS - transit_count
        per_run_transit = min(MAX_API_ITEMS_PER_RUN, remaining_transit)
        stop_type_cycle = [
            ("NaptanMetroStation", (51.515, -0.13)),
            ("NaptanRailStation", (51.503, -0.112)),
            ("NaptanBusCoachStation", (51.510, -0.090)),
        ]
        cycle_index = transit_count // MAX_API_ITEMS_PER_RUN % len(stop_type_cycle)
        stop_types, (t_lat, t_lon) = stop_type_cycle[cycle_index]
        print(f"Fetching up to {per_run_transit} TfL stops (need {remaining_transit} more) "
              f"using stop types [{stop_types}] near ({t_lat}, {t_lon}).")
        fetch_transit_stops(
            conn,
            lat=t_lat,
            lon=t_lon,
            max_items=per_run_transit,
            stop_types=stop_types
        )
        print("Re-run the script to accumulate at least 100 transit stops.")
    else:
        print("TransitStops already satisfies the minimum rows.")

    # Build weather date list
    print("Preparing weather date list...")
    crime_months = get_unique_crime_months(conn)
    DATES = []

    for m in crime_months:
        DATES.extend(expand_month_to_dates(m))

    DATES = sorted(set(DATES))

    # Fetch weather only if needed
    if weather_count < MIN_WEATHER_ROWS:
        remaining = MIN_WEATHER_ROWS - weather_count
        per_run = min(MAX_API_ITEMS_PER_RUN, remaining)
        print(f"Fetching weather chunk (max {per_run} rows). Need {remaining} more to reach {MIN_WEATHER_ROWS}.")
    else:
        per_run = MAX_API_ITEMS_PER_RUN
        print("WeatherData already meets minimum rows; refreshing limited chunk for recency.")
    fetch_weather_for_all_locations(conn, dates=DATES, max_new_records=per_run)
    if weather_count < MIN_WEATHER_ROWS:
        print("Re-run the script to continue gathering weather records without exceeding per-run limits.")

    # Analysis
    print("Computing analysis...")
    df_weather = calculate_crimes_by_weather(conn)
    df_temp = calculate_crimes_by_temperature_bins(conn)
    df_types = calculate_crime_type_distribution(conn)
    df_wind = calculate_crimes_vs_wind(conn)
    df_rain = calculate_precipitation_effect(conn)
    df_transit = calculate_crimes_near_transit(conn)

    # Visualizations
    print("Generating visualizations...")
    visualize_results(df_weather, df_temp, df_types, df_wind, df_rain, df_transit)

    print(f"Writing analysis summary to {REPORT_PATH} ...")
    write_analysis_report(REPORT_PATH, df_weather, df_temp, df_types, df_wind, df_rain, df_transit)

    print("Done! Visualizations saved.")


if __name__ == "__main__":
    main()
