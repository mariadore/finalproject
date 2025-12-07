import requests
import time
from datetime import datetime
from db_utils import insert_weather

OWM_URL = "https://api.openweathermap.org/data/2.5/onecall/timemachine"


def fetch_weather(lat, lon, date, api_key):
    """
    Fetch daily historical weather.
    date: YYYY-MM-DD
    """
    dt = int(datetime.strptime(date, "%Y-%m-%d").timestamp())

    params = {
        "lat": lat,
        "lon": lon,
        "dt": dt,
        "appid": api_key,
        "units": "metric"
    }

    resp = requests.get(OWM_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly", [])
    if not hourly:
        return None

    temps = [h["temp"] for h in hourly]
    hums = [h["humidity"] for h in hourly]
    winds = [h["wind_speed"] for h in hourly]

    weather_main = hourly[0]["weather"][0]["main"]

    precip = 0
    for h in hourly:
        precip += h.get("rain", {}).get("1h", 0) + h.get("snow", {}).get("1h", 0)

    return {
        "temp_c": sum(temps)/len(temps),
        "temp_min_c": min(temps),
        "temp_max_c": max(temps),
        "humidity": sum(hums)/len(hums),
        "wind_speed": sum(winds)/len(winds),
        "precip_mm": precip,
        "weather_main": weather_main
    }


def fetch_weather_for_all_locations(conn, api_key, dates, max_items=25):
    """
    Fetch weather for up to max_items LocationData rows.
    """

    cur = conn.cursor()
    cur.execute("SELECT location_id, lat, lon FROM LocationData LIMIT ?", (max_items,))
    rows = cur.fetchall()

    for location_id, lat, lon in rows:
        for date in dates:
            w = fetch_weather(lat, lon, date, api_key)
            if w:
                weather_row = {
                    "location_id": location_id,
                    "date": date,
                    "temp_c": w["temp_c"],
                    "temp_min_c": w["temp_min_c"],
                    "temp_max_c": w["temp_max_c"],
                    "precip_mm": w["precip_mm"],
                    "wind_speed": w["wind_speed"],
                    "humidity": w["humidity"],
                    "weather_main": w["weather_main"]
                }
                insert_weather(conn, weather_row)

            time.sleep(1)
