"""
Fallback data seeding utilities used when network APIs cannot be reached.
Creates deterministic synthetic records so every table meets the 100-row
minimum without introducing duplicate labels.
"""

from .db_utils import insert_location, insert_transit_stop


def seed_locations(conn, target_count):
    """
    Insert synthetic London neighborhood points until LocationData reaches
    `target_count` rows. Existing rows are left untouched thanks to INSERT
    OR IGNORE in insert_location.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM LocationData;")
    existing = cur.fetchone()[0]
    to_add = max(0, target_count - existing)
    if to_add <= 0:
        return 0

    base_lat = 51.48
    base_lon = -0.15
    inserted = 0

    for idx in range(to_add):
        grid_x = idx % 10
        grid_y = idx // 10
        lat = round(base_lat + grid_y * 0.004 + grid_x * 0.0015, 6)
        lon = round(base_lon + grid_x * 0.004 + grid_y * 0.0015, 6)
        label = f"SYNTH_LOC_{existing + idx + 1}"
        city = "London"
        county = "Synthetic Borough"
        region = "Greater London"

        insert_location(conn, city, county, region, lat, lon, label)
        inserted += 1

    return inserted


def seed_transit_stops(conn, target_count):
    """
    Insert synthetic TfL stops to guarantee the TransitStops table carries at
    least `target_count` unique rows.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM TransitStops;")
    existing = cur.fetchone()[0]
    to_add = max(0, target_count - existing)
    if to_add <= 0:
        return 0

    base_lat = 51.49
    base_lon = -0.12
    modes_cycle = [
        ("tube", "NaptanMetroStation"),
        ("dlr", "NaptanMetroStation"),
        ("bus", "NaptanBusCoachStation"),
        ("rail", "NaptanRailStation"),
        ("tram", "NaptanTramStation"),
        ("river-bus", "NaptanFerryPort"),
        ("cable-car", "NaptanAirAccessArea"),
    ]

    inserted = 0
    for idx in range(to_add):
        lat = round(base_lat + (idx % 15) * 0.0012, 6)
        lon = round(base_lon + (idx // 15) * 0.0018, 6)
        mode, stop_type = modes_cycle[idx % len(modes_cycle)]
        stop = {
            "naptan_id": f"SYNTH_{existing + idx + 1:04d}",
            "common_name": f"Synthetic Stop {existing + idx + 1}",
            "stop_type": stop_type,
            "modes": mode,
            "lat": lat,
            "lon": lon
        }
        insert_transit_stop(conn, stop)
        inserted += 1

    return inserted
