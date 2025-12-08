import pandas as pd



def calculate_crimes_by_weather(conn):
    """
    JOIN CrimeData → LocationData → WeatherData
    Group by weather_main
    """
    query = """
        SELECT 
            W.weather_main,
            COUNT(C.id) AS total_crimes,
            COUNT(C.id) * 1.0 / COUNT(DISTINCT W.date) AS avg_crimes_per_day
        FROM CrimeData C
        JOIN WeatherData W ON C.location_id = W.location_id 
                           AND C.month = SUBSTR(W.date, 1, 7)
        GROUP BY W.weather_main
        ORDER BY total_crimes DESC;
    """

    df = pd.read_sql_query(query, conn)
    return df



def calculate_crimes_by_temperature_bins(conn):
    """
    Group crimes by temperature_bin:
        <5°C, 5–10°C, 10–15°C, 15–20°C, >20°C
    """
    query = """
        SELECT
            CASE
                WHEN W.temp_c < 5 THEN '<5°C'
                WHEN W.temp_c BETWEEN 5 AND 10 THEN '5–10°C'
                WHEN W.temp_c BETWEEN 10 AND 15 THEN '10–15°C'
                WHEN W.temp_c BETWEEN 15 AND 20 THEN '15–20°C'
                ELSE '>20°C'
            END AS temp_bin,
            COUNT(C.id) AS total_crimes
        FROM CrimeData C
        JOIN WeatherData W ON C.location_id = W.location_id
                           AND C.month = SUBSTR(W.date, 1, 7)
        GROUP BY temp_bin
        ORDER BY total_crimes DESC;
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
                           AND C.month = SUBSTR(W.date, 1, 7)
        GROUP BY W.weather_main, C.category
        ORDER BY W.weather_main, crime_count DESC;
    """

    df = pd.read_sql_query(query, conn)
    return df
