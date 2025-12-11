import requests
from datetime import datetime
from .db_utils import insert_weather, get_locations_missing_weather


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(lat, lon, date):
    """Fetch daily weather from Open-Meteo."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "windspeed_10m_max",
            "precipitation_sum",
            "rain_sum",
            "showers_sum",
            "snowfall_sum",
            "weathercode"
        ],
        "timezone": "UTC",
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # If no data
    if "daily" not in data:
        return None

    daily = data["daily"]

    return {
        "date": date,
        "temp_max": daily.get("temperature_2m_max", [None])[0],
        "temp_min": daily.get("temperature_2m_min", [None])[0],
        "windspeed": daily.get("windspeed_10m_max", [None])[0],
        "precip": daily.get("precipitation_sum", [None])[0],
        "rain": daily.get("rain_sum", [None])[0],
        "showers": daily.get("showers_sum", [None])[0],
        "snow": daily.get("snowfall_sum", [None])[0],
        "weathercode": daily.get("weathercode", [None])[0],
    }


def fetch_weather_for_all_locations(conn, dates, max_items=5):
    """
    Fetch weather for each unique location in DB.
    """
    locations = get_locations_missing_weather(conn, limit=max_items)

    print(f"Fetching weather for {len(locations)} locations...")

    for location_id, lat, lon in locations:
        for date in dates:
            weather = fetch_weather(lat, lon, date)
            if weather:
                insert_weather(
                    conn,
                    location_id,
                    weather["date"],
                    weather["temp_max"],
                    weather["temp_min"],
                    weather["windspeed"],
                    weather["precip"],
                    weather["rain"],
                    weather["showers"],
                    weather["snow"],
                    weather["weathercode"]
                )
    print("Weather fetch complete.")
