import requests
import time
from .db_utils import insert_weather

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_weather(lat, lon, date):
    """
    Fetch historical weather from Open-Meteo Archive API.
    date format: YYYY-MM-DD
    """

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
        "timezone": "UTC"
    }

    resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    hourly = data.get("hourly", {})
    temps = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    precip = hourly.get("precipitation", [])
    wind = hourly.get("wind_speed_10m", [])
    weather_codes = hourly.get("weather_code", [])

    if not temps:
        print(f"⚠️ No hourly weather found for {date}")
        return None

    temp_avg = sum(temps) / len(temps)
    temp_min = min(temps)
    temp_max = max(temps)
    precip_mm = sum(precip)
    humidity_avg = sum(humidity) / len(humidity)
    wind_avg = sum(wind) / len(wind)

    WEATHER_CODE_MAP = {
        0: "Clear",
        1: "Mainly Clear",
        2: "Partly Cloudy",
        3: "Overcast",
        45: "Foggy",
        48: "Rime Fog",
        51: "Light Drizzle",
        61: "Rain",
        71: "Snow",
        80: "Rain Showers",
        95: "Thunderstorm"
    }

    if weather_codes:
        wc = weather_codes[0]
        weather_main = WEATHER_CODE_MAP.get(wc, "Unknown")
    else:
        weather_main = "Unknown"

    return {
        "temp_c": temp_avg,
        "temp_min_c": temp_min,
        "temp_max_c": temp_max,
        "precip_mm": precip_mm,
        "wind_speed": wind_avg,
        "humidity": humidity_avg,
        "weather_main": weather_main
    }


def fetch_weather_for_all_locations(conn, dates, max_items=25):
    """
    Fetch historical weather for all locations using Open-Meteo Archive API.
    """

    cur = conn.cursor()
    cur.execute("SELECT location_id, lat, lon FROM LocationData LIMIT ?", (max_items,))
    rows = cur.fetchall()

    for location_id, lat, lon in rows:
        for date in dates:
            weather = fetch_weather(lat, lon, date)

            if weather:
                insert_weather(conn, {
                    "location_id": location_id,
                    "date": date,
                    "temp_c": weather["temp_c"],
                    "temp_min_c": weather["temp_min_c"],
                    "temp_max_c": weather["temp_max_c"],
                    "precip_mm": weather["precip_mm"],
                    "wind_speed": weather["wind_speed"],
                    "humidity": weather["humidity"],
                    "weather_main": weather["weather_main"]
                })

            time.sleep(0.25)  # prevent spamming the API
