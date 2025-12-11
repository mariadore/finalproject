import requests
import time
from .db_utils import insert_weather

TOMTOM_WEATHER_KEY = "tfFYIYGPk25q0LqtkS2chNGRsdPWLA2y"
TOMTOM_WEATHER_URL = "https://api.tomtom.com/weather/1.0/weather/history"


def fetch_weather(lat, lon, date):
    """
    Fetch historical weather for a specific location on a specific date.
    Uses TomTom Historical Weather API.
    """

    params = {
        "key": TOMTOM_WEATHER_KEY,
        "lat": lat,
        "lon": lon,
        "date": date,
        "fields": "temp,humidity,windSpeed,precipitation,cloudCover"
    }

    resp = requests.get(TOMTOM_WEATHER_URL, params=params, timeout=30)

    if resp.status_code == 429:
        print("Weather API rate limited: waiting 2 seconds...")
        time.sleep(2)
        resp = requests.get(TOMTOM_WEATHER_URL, params=params, timeout=30)

    resp.raise_for_status()
    data = resp.json()

    # TomTom structure → extract hourly weather
    hours = data.get("timelines", {}).get("hourly", [])
    if not hours:
        return None

    temps = [h["values"]["temp"] for h in hours]
    humid = [h["values"]["humidity"] for h in hours]
    wind = [h["values"]["windSpeed"] for h in hours]
    precip = [h["values"]["precipitation"] for h in hours]

    weather_main = (
        "Rain" if sum(precip) > 1 else
        "Windy" if sum(wind)/len(wind) > 15 else
        "Clear" if max(temps) > 15 else
        "Cloudy"
    )

    return {
        "temp_c": sum(temps)/len(temps),
        "temp_min_c": min(temps),
        "temp_max_c": max(temps),
        "precip_mm": sum(precip),
        "wind_speed": sum(wind)/len(wind),
        "humidity": sum(humid)/len(humid),
        "weather_main": weather_main
    }


def fetch_weather_for_all_locations(conn, dates, max_items=25):
    """
    Fetch TomTom historical weather for each location and date.
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

            time.sleep(0.2)
