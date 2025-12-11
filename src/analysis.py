import pandas as pd



def calculate_crimes_by_weather(conn):
    """
    Join WeatherData → CrimeData to ensure every weather type
    appears even if it has zero linked crimes.
    """
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


def calculate_crimes_near_transit(conn):
    """
    Count crimes occurring near TfL transit stops (bounding-box proximity).
    Always include transit modes even if zero crimes were observed so the
    visualization has data to show.
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

    df["primary_mode"] = df["modes"].fillna("unknown").apply(lambda m: m.split(",")[0] if m else "unknown")
    grouped = (
        df.groupby("primary_mode", as_index=False)
          .agg(
              crime_count=("crime_count", "sum"),
              stop_count=("stop_id", "count")
          )
          .sort_values("crime_count", ascending=False)
          .reset_index(drop=True)
    )
    grouped["avg_crimes_per_stop"] = grouped["crime_count"] / grouped["stop_count"].clip(lower=1)
    return grouped
