import requests
import time
from .db_utils import insert_weather

# Hard-coded Weatherstack API key
WEATHERSTACK_API_KEY = "YOUR_WEATHERSTACK_KEY_HERE"

WEATHERSTACK_URL = "http://api.weatherstack.com/historical"


def fetch_weather(lat, lon, date, api_key=WEATHERSTACK_API_KEY):
    """
    Fetch historical weather from Weatherstack API.
    date format: 'YYYY-MM-DD'
    """
    params = {
        "access_key": api_key,
        "query": f"{lat},{lon}",
        "historical_date": date,
        "hourly": "1"
    }

    resp = requests.get(WEATHERSTACK_URL, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    
    # Weatherstack error field
    if "error" in data:
        print("❌ Weatherstack error:", data["error"])
        return None

    historical = data.get("historical", {})
    if date not in historical:
        print(f"⚠️ No historical weather found for {date}")
        return None

    day = historical[date]

    # Extract useful values
    temp_c = day.get("avgtemp")
    temp_min = day.get("mintemp")
    temp_max = day.get("maxtemp")
    humidity = day.get("avghumidity")
    precip_mm = day.get("precip")
    
    # Weather conditions from "hourly"
    hourly = day.get("hourly", [])
    if hourly:
        weather_main = hourly[0].get("weather_descriptions", ["Unknown"])[0]
        wind_speed = hourly[0].get("windspeed")
    else:
        weather_main = "Unknown"
        wind_speed = None

    return {
        "temp_c": temp_c,
        "temp_min_c": temp_min,
        "temp_max_c": temp_max,
        "humidity": humidity,
        "precip_mm": precip_mm,
        "wind_speed": wind_speed,
        "weather_main": weather_main
    }


def fetch_weather_for_all_locations(conn, dates, api_key=WEATHERSTACK_API_KEY, max_items=25):
    """
    Fetch historical weather for up to max_items rows in LocationData.
    Inserts into WeatherData table.
    """

    cur = conn.cursor()
    cur.execute("SELECT location_id, lat, lon FROM LocationData LIMIT ?", (max_items,))
    rows = cur.fetchall()

    for location_id, lat, lon in rows:
        for date in dates:
            weather = fetch_weather(lat, lon, date, api_key)

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

            time.sleep(1)  # avoid rate limits
