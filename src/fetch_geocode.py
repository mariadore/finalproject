# reverse_geocode.py
import requests
from store_data import store_location_data

API_KEY = "e1534616c63441ebf4d00f2ed847b6d5"

def reverse_geocode(lat, lon, crime_id):
    url = f"http://api.positionstack.com/v1/reverse?access_key={API_KEY}&query={lat},{lon}"
    r = requests.get(url)
    d = r.json()["data"][0]

    geo = {
        "city": d.get("locality"),
        "county": d.get("county"),
        "region": d.get("region"),
        "label": d.get("label")
    }

    store_location_data(geo, crime_id)
    return geo
