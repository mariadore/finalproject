import argparse
import calendar
import os
from src.db_utils import (
    set_up_database,
    release_default_location_links,
    get_transit_stop_count,
    get_api_cursor,
    set_api_cursor
)
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
    calculate_crimes_near_transit,
    calculate_transit_stop_hotspots
)
from src.report import write_analysis_report
from src.visualize import visualize_results
from src.seed import seed_locations, seed_transit_stops

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


def parse_args():
    parser = argparse.ArgumentParser(description="SI 201 final project pipeline runner.")
    parser.add_argument("--month", default="2023-09",
                        help="Crime month (YYYY-MM) to download per run.")
    parser.add_argument("--allow-seed", action="store_true",
                        help="Allow synthetic data seeding when APIs cannot satisfy requirements.")
    parser.add_argument("--show-plots", action="store_true",
                        help="Display matplotlib windows in addition to saving PNGs.")
    return parser.parse_args()


def _read_counts(cur):
    cur.execute("SELECT COUNT(*) FROM CrimeData;")
    crime = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM LocationData WHERE label != 'DEFAULT_LONDON';")
    loc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM WeatherData;")
    weather = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM TransitStops;")
    transit = cur.fetchone()[0]
    return {
        "CrimeData": crime,
        "LocationData": loc,
        "WeatherData": weather,
        "TransitStops": transit
    }


def _print_run_summary(start_counts, end_counts, per_run_details=None):
    print("\n=== Run Summary ===")
    rows = [
        ("CrimeData", MIN_CRIME_ROWS),
        ("LocationData", MIN_LOCATION_ROWS),
        ("WeatherData", MIN_WEATHER_ROWS),
        ("TransitStops", MIN_TRANSIT_ROWS)
    ]
    for table, target in rows:
        before = start_counts.get(table, 0)
        after = end_counts.get(table, 0)
        delta = after - before
        status = "✅" if after >= target else "⚠"
        detail = ""
        if per_run_details and table in per_run_details:
            detail = per_run_details[table]
        print(f"{status} {table}: {after}/{target} rows (this run +{max(delta, 0)}) {detail}".strip())
    print("====================\n")


def main(month="2023-09", allow_seed=False, show_plots=False):
    print("Setting up database...")
    db_path, conn = set_up_database()

    # Check existing table counts
    cur = conn.cursor()
    start_counts = _read_counts(cur)
    crime_count = start_counts["CrimeData"]
    loc_count = start_counts["LocationData"]
    weather_count = start_counts["WeatherData"]
    transit_count = start_counts["TransitStops"]

    # Fetch crime data only if needed
    per_run_notes = {}

    if crime_count < MIN_CRIME_ROWS:
        remaining = MIN_CRIME_ROWS - crime_count
        fetch_limit = min(MAX_API_ITEMS_PER_RUN, remaining)
        print(f"Fetching up to {fetch_limit} crimes (need {remaining} more to hit {MIN_CRIME_ROWS}).")
        before_crime = crime_count
        fetch_and_store_crimes(conn, month, max_items=fetch_limit)
        cur.execute("SELECT COUNT(*) FROM CrimeData;")
        crime_count = cur.fetchone()[0]
        inserted = max(crime_count - before_crime, 0)
        per_run_notes["CrimeData"] = f"[fetched {inserted}/{fetch_limit}]"
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
        before_loc = loc_count
        geocode_and_attach_locations(conn, max_items=per_run)
        cur.execute("SELECT COUNT(*) FROM LocationData WHERE label != 'DEFAULT_LONDON';")
        loc_count = cur.fetchone()[0]
        per_run_notes["LocationData"] = f"[geocoded {max(loc_count - before_loc, 0)}/{per_run}]"
        if loc_count < MIN_LOCATION_ROWS:
            print("Re-run the script to continue building LocationData via TomTom.")
    else:
        print("LocationData already satisfies the minimum rows.")

    assign_default_location(conn)

    if loc_count < MIN_LOCATION_ROWS and allow_seed:
        print("Seeding synthetic locations to reach required minimum (allow-seed enabled).")
        added = seed_locations(conn, MIN_LOCATION_ROWS)
        if added:
            print(f"Inserted {added} synthetic fallback locations.")
        cur.execute("SELECT COUNT(*) FROM LocationData WHERE label != 'DEFAULT_LONDON';")
        loc_count = cur.fetchone()[0]
        assign_default_location(conn)
    else:
        if loc_count < MIN_LOCATION_ROWS:
            print("LocationData still below requirement. Re-run the script to gather more TomTom results.")

    # Transit stops (TfL)
    if transit_count < MIN_TRANSIT_ROWS:
        remaining_transit = MIN_TRANSIT_ROWS - transit_count
        per_run_transit = min(MAX_API_ITEMS_PER_RUN, remaining_transit)
        stop_type_cycle = [
            {
                "stop_types": "NaptanMetroStation",
                "coords": (51.515, -0.13),
                "modes": ("tube", "dlr", "overground")
            },
            {
                "stop_types": "NaptanRailStation",
                "coords": (51.503, -0.112),
                "modes": ("overground",)
            },
            {
                "stop_types": "NaptanBusCoachStation",
                "coords": (51.510, -0.090),
                "modes": ("bus", "coach")
            },
            {
                "stop_types": "NaptanTramStation",
                "coords": (51.374, -0.10),
                "modes": ("tram",)
            },
            {
                "stop_types": None,
                "coords": (51.509, -0.017),
                "modes": ("dlr",)
            },
            {
                "stop_types": None,
                "coords": (51.500, 0.003),
                "modes": ("cable-car",)
            },
            {
                "stop_types": None,
                "coords": (51.507, -0.02),
                "modes": ("river-bus", "river-tour")
            },
        ]
        cursor_value = get_api_cursor(conn, "transit_cycle", default="0")
        try:
            cycle_index = int(cursor_value)
        except (TypeError, ValueError):
            cycle_index = 0
        config = stop_type_cycle[cycle_index % len(stop_type_cycle)]
        stop_types = config["stop_types"]
        t_lat, t_lon = config["coords"]
        cycle_modes = config.get("modes")
        print(f"Fetching up to {per_run_transit} TfL stops (need {remaining_transit} more) "
              f"using stop types [{stop_types}] near ({t_lat}, {t_lon}).")
        before_transit = transit_count
        fetch_transit_stops(
            conn,
            lat=t_lat,
            lon=t_lon,
            max_items=per_run_transit,
            stop_types=stop_types,
            modes=cycle_modes or ("tube", "dlr", "overground", "bus")
        )
        set_api_cursor(conn, "transit_cycle", str((cycle_index + 1) % len(stop_type_cycle)))
        print("Re-run the script to accumulate at least 100 transit stops.")
        transit_count = get_transit_stop_count(conn)
        per_run_notes["TransitStops"] = f"[inserted {max(transit_count - before_transit, 0)}/{per_run_transit}]"
    else:
        print("TransitStops already satisfies the minimum rows.")

    if transit_count < MIN_TRANSIT_ROWS and allow_seed:
        print("Seeding synthetic transit stops to reach required minimum (allow-seed enabled).")
        added_transit = seed_transit_stops(conn, MIN_TRANSIT_ROWS)
        if added_transit:
            print(f"Inserted {added_transit} synthetic stops.")
        transit_count = get_transit_stop_count(conn)
    elif transit_count < MIN_TRANSIT_ROWS:
        print("TransitStops still below requirement. Re-run the script after data collection limits reset.")

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
    before_weather = weather_count
    fetch_weather_for_all_locations(conn, dates=DATES, max_new_records=per_run)
    cur.execute("SELECT COUNT(*) FROM WeatherData;")
    weather_count = cur.fetchone()[0]
    per_run_notes["WeatherData"] = f"[inserted {max(weather_count - before_weather, 0)}/{per_run}]"
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
    df_transit_hotspots = calculate_transit_stop_hotspots(conn)

    # Visualizations
    print("Generating visualizations...")
    visualize_results(
        df_weather,
        df_temp,
        df_types,
        df_wind,
        df_rain,
        df_transit,
        df_transit_hotspots,
        show_plots=show_plots
    )

    print(f"Writing analysis summary to {REPORT_PATH} ...")
    write_analysis_report(REPORT_PATH, df_weather, df_temp, df_types, df_wind, df_rain, df_transit)

    end_counts = _read_counts(cur)
    _print_run_summary(start_counts, end_counts, per_run_details=per_run_notes)
    print("Done! Visualizations saved.")


if __name__ == "__main__":
    args = parse_args()
    main(month=args.month, allow_seed=args.allow_seed, show_plots=args.show_plots)
