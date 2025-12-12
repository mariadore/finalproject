"""
analysis.py

This module contains all analysis functions used to derive insights from the
WeatherData, CrimeData, and TransitStops tables. Each function returns a
Pandas DataFrame that can be used directly for visualization or statistical
interpretation.
"""

import pandas as pd


# Crime and weather relationship analysis
def calculate_crimes_by_weather(conn):
    """
    Join WeatherData → CrimeData to ensure every weather type
    appears even if it has zero linked crimes.
    """
    # SQL: Aggregate crimes grouped by weather category with per-day normalization

    query = """
        SELECT 
            W.weather_main,
            COUNT(C.id) AS total_crimes,
            CASE
                WHEN COUNT(DISTINCT W.date) = 0 THEN 0
                ELSE COUNT(C.id) * 1.0 / COUNT(DISTINCT W.date)
            END AS avg_crimes_per_day
        FROM WeatherData W
        LEFT JOIN CrimeData C ON C.location_id = W.location_id 
                             AND C.crime_date = W.date
        GROUP BY W.weather_main
        ORDER BY avg_crimes_per_day DESC;
    """

    df = pd.read_sql_query(query, conn)
    return df


def calculate_crimes_by_temperature_bins(conn):
    """
    Return per-location, per-day crime counts linked to the actual average
    temperature for that day. This gives many data points for visualization.
    """
    # SQL: Return crime/temperature pairs per location per day
    query = """
        SELECT
            W.location_id,
            W.date AS weather_date,
            W.temp_c,
            COUNT(C.id) AS total_crimes
        FROM WeatherData W
        LEFT JOIN CrimeData C ON C.location_id = W.location_id
                             AND C.crime_date = W.date
        WHERE W.temp_c IS NOT NULL
        GROUP BY W.location_id, W.date, W.temp_c
        ORDER BY W.date ASC, W.location_id ASC;
    """

    df = pd.read_sql_query(query, conn)
    return df

def calculate_crime_type_distribution(conn):
    """
    Distribution of crime categories under each weather type.
    """
    # SQL: Get distribution of crime categories under each weather type
    query = """
        SELECT
            W.weather_main,
            C.category,
            COUNT(C.id) AS crime_count
        FROM CrimeData C
        JOIN WeatherData W ON C.location_id = W.location_id
                           AND C.crime_date = W.date
        GROUP BY W.weather_main, C.category
        ORDER BY W.weather_main, crime_count DESC;
    """

    df = pd.read_sql_query(query, conn)
    return df


def calculate_crimes_vs_wind(conn):
    query = """
        SELECT 
            ROUND(W.wind_speed, 1) AS wind_bin,
            COUNT(C.id) AS crime_count
        FROM CrimeData C
        JOIN WeatherData W ON C.location_id = W.location_id
                           AND C.crime_date = W.date
        GROUP BY wind_bin
        ORDER BY wind_bin;
    """

    return pd.read_sql_query(query, conn)


def calculate_precipitation_effect(conn):
    query = """
        SELECT
            CASE
                WHEN W.precip_mm = 0 THEN 'Dry'
                WHEN W.precip_mm BETWEEN 0.1 AND 2 THEN 'Light Rain'
                ELSE 'Heavy Rain'
            END AS rain_level,
            COUNT(C.id) AS crime_count
        FROM CrimeData C
        JOIN WeatherData W ON C.location_id = W.location_id
                           AND C.crime_date = W.date
        GROUP BY rain_level
        ORDER BY crime_count DESC;
    """

    return pd.read_sql_query(query, conn)


STOP_TYPE_MODE_MAP = {
    "NaptanMetroStation": "tube",
    "NaptanRailStation": "rail",
    "NaptanBusCoachStation": "bus",
    "NaptanPublicBusCoachTram": "bus",
    "NaptanTramStation": "tram",
    "NaptanFerryPort": "river",
    "NaptanAirAccessArea": "air"
}
"""
EXPECTED_TRANSIT_MODES defines all transit categories that should appear in
charts even when the dataset contains zero stops or zero crimes for them.
This ensures consistent visualization across incomplete data scenarios.
"""

EXPECTED_TRANSIT_MODES = sorted({
    "tube", "rail", "bus", "tram", "river", "air",
    "overground", "dlr", "coach", "river-bus", "river-tour",
    "cable-car", "national-rail"
} | set(STOP_TYPE_MODE_MAP.values()))
# Transit stop crime analysis (TfL multi-mode support)

def _split_modes(modes_str, stop_type):
    """
    Given a TfL 'modes' string and a fallback stop_type, return a cleaned list
    of individual modes. If the modes string is empty or null, fall back to the
    STOP_TYPE_MODE_MAP mapping. Ensures consistent mode parsing.
    """
    if not modes_str:
        fallback = STOP_TYPE_MODE_MAP.get(stop_type)
        return [fallback] if fallback else []
    modes = [m.strip() for m in modes_str.split(",") if m.strip()]
    if modes:
        return modes
    fallback = STOP_TYPE_MODE_MAP.get(stop_type)
    return [fallback] if fallback else []


def calculate_crimes_near_transit(conn):
    """
    Count crimes occurring near TfL transit stops (bounding-box proximity).
    Crimes at multi-mode stops are distributed across each mode they serve so the
    visualization can highlight more than one transit category.
    """
    query = """
        SELECT
            T.stop_id,
            T.common_name,
            T.modes,
            T.stop_type,
            COALESCE(COUNT(C.id), 0) AS crime_count
        FROM TransitStops T
        LEFT JOIN CrimeData C
          ON C.latitude IS NOT NULL
         AND C.longitude IS NOT NULL
         AND ABS(C.latitude - T.lat) <= 0.01
         AND ABS(C.longitude - T.lon) <= 0.01
        GROUP BY T.stop_id
        ORDER BY crime_count DESC;
    """

    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df

    df["mode_list"] = df.apply(lambda row: _split_modes(row["modes"], row["stop_type"]), axis=1)
    df["mode_count"] = df["mode_list"].apply(lambda lst: len(lst) if lst else 1)
    df["mode_list"] = df["mode_list"].apply(lambda lst: lst if lst else ["unknown"])

    exploded = df.explode("mode_list").rename(columns={"mode_list": "mode"}).copy()
    exploded["weighted_crimes"] = exploded["crime_count"] / exploded["mode_count"].clip(lower=1)

    grouped = (
        exploded.groupby("mode", as_index=False)
                .agg(
                    crime_count=("weighted_crimes", "sum"),
                    stop_count=("stop_id", "nunique")
                )
                .sort_values("crime_count", ascending=False)
                .reset_index(drop=True)
    )
    grouped["crime_count"] = grouped["crime_count"].round(2)
    grouped["avg_crimes_per_stop"] = grouped["crime_count"] / grouped["stop_count"].clip(lower=1)

    # Ensure every expected mode appears in visualization even if zero stops/crimes yet.
    missing_modes = [m for m in EXPECTED_TRANSIT_MODES if m not in grouped["mode"].values]
    if missing_modes:
        filler = pd.DataFrame({
            "mode": missing_modes,
            "crime_count": [0.0] * len(missing_modes),
            "stop_count": [0] * len(missing_modes),
            "avg_crimes_per_stop": [0.0] * len(missing_modes)
        })
        grouped = pd.concat([grouped, filler], ignore_index=True)
        grouped = grouped.sort_values("crime_count", ascending=False).reset_index(drop=True)

    return grouped


def calculate_transit_stop_hotspots(conn, limit=15):
    """
    Return the top transit stops (TfL) ranked by number of nearby crimes.
    """
    query = """
        SELECT
            T.stop_id,
            T.common_name,
            T.stop_type,
            T.modes,
            T.lat,
            T.lon,
            COUNT(C.id) AS crime_count
        FROM TransitStops T
        LEFT JOIN CrimeData C
          ON C.latitude IS NOT NULL
         AND C.longitude IS NOT NULL
         AND ABS(C.latitude - T.lat) <= 0.01
         AND ABS(C.longitude - T.lon) <= 0.01
        GROUP BY T.stop_id
        ORDER BY crime_count DESC, T.common_name ASC
        LIMIT ?
    """

    return pd.read_sql_query(query, conn, params=(limit,))
