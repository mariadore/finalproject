import requests
from .db_utils import insert_transit_stop

TFL_BASE_URL = "https://api.tfl.gov.uk/StopPoint"
TFL_APP_ID = "514bf5ddd2804e52ad81eb6360271a78"
TFL_APP_KEY = "a2002e2b19404129aa95c5ac91f55c46"


def _normalize_stop(raw):
    if not raw:
        return None

    modes = raw.get("modes") or []
    return {
        "naptan_id": raw.get("naptanId"),
        "common_name": raw.get("commonName"),
        "stop_type": raw.get("stopType"),
        "modes": ",".join(modes),
        "lat": raw.get("lat"),
        "lon": raw.get("lon")
    }


def fetch_transit_stops(conn,
                        lat=51.509865,
                        lon=-0.118092,
                        radius=1500,
                        max_items=25,
                        modes=("tube", "dlr", "overground", "bus"),
                        stop_types="NaptanMetroStation,NaptanRailStation,NaptanBusCoachStation"):
    """
    Fetch TfL StopPoints around a central coordinate and store them.
    """
    params = {
        "lat": lat,
        "lon": lon,
        "radius": radius,
        "app_id": TFL_APP_ID,
        "app_key": TFL_APP_KEY
    }
    if stop_types:
        params["stopTypes"] = stop_types
    if modes:
        params["modes"] = ",".join(modes)

    try:
        resp = requests.get(TFL_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"TfL API error: {exc}")
        return 0

    data = resp.json() or {}
    stop_points = data.get("stopPoints") or []

    inserted = 0
    for raw in stop_points:
        if inserted >= max_items:
            break
        stop = _normalize_stop(raw)
        if stop and stop["naptan_id"]:
            insert_transit_stop(conn, stop)
            inserted += 1

    print(f"Inserted {inserted} TfL stops.")
    return inserted
