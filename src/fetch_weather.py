import requests
from .db_utils import insert_weather, get_locations_missing_weather


OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_CODE_MAP = {
    0: "Clear",
    1: "Mainly Clear",
    2: "Partly Cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Fog",
    51: "Light Drizzle",
    53: "Drizzle",
    55: "Heavy Drizzle",
    56: "Freezing Drizzle",
    57: "Freezing Drizzle",
    61: "Light Rain",
    63: "Rain",
    65: "Heavy Rain",
    66: "Freezing Rain",
    67: "Freezing Rain",
    71: "Light Snow",
    73: "Snow",
    75: "Heavy Snow",
    77: "Snow Grains",
    80: "Rain Showers",
    81: "Rain Showers",
    82: "Heavy Rain Showers",
    85: "Snow Showers",
    86: "Heavy Snow Showers",
    95: "Thunderstorm",
    96: "Thunderstorm",
    99: "Thunderstorm"
}


def _first_daily_value(daily_data, key):
    values = daily_data.get(key)
    if isinstance(values, list) and values:
        return values[0]
    return None


def _derive_weather_main(code):
    if code is None:
        return "Unknown"
    return WEATHER_CODE_MAP.get(int(code), "Unknown")


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

    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as exc:
        print(f"Open-Meteo error ({exc}); skipping {date} for {lat},{lon}")
        return None

    data = resp.json()

    # If no data
    if "daily" not in data:
        return None

    daily = data["daily"]

    temp_max = _first_daily_value(daily, "temperature_2m_max")
    temp_min = _first_daily_value(daily, "temperature_2m_min")
    if temp_max is not None and temp_min is not None:
        temp_c = (temp_max + temp_min) / 2
    else:
        temp_c = temp_max if temp_max is not None else temp_min

    weather_code = _first_daily_value(daily, "weathercode")

    return {
        "date": date,
        "temp_c": temp_c,
        "temp_min_c": temp_min,
        "temp_max_c": temp_max,
        "wind_speed": _first_daily_value(daily, "windspeed_10m_max"),
        "precip_mm": _first_daily_value(daily, "precipitation_sum"),
        "humidity": None,
        "weather_main": _derive_weather_main(weather_code)
    }


def fetch_weather_for_all_locations(conn, dates, max_items=5):
    """
    Fetch weather for each unique location in DB.
    """
    if not dates:
        print("No dates supplied → skipping weather fetch.")
        return

    locations = get_locations_missing_weather(conn, limit=max_items)

    print(f"Fetching weather for {len(locations)} locations...")

    for location_id, lat, lon in locations:
        for date in dates:
            weather = fetch_weather(lat, lon, date)
            if weather:
                weather["location_id"] = location_id
                insert_weather(conn, weather)
    print("Weather fetch complete.")
