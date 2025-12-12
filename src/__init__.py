"""
src/__init__.py

Central export hub for all major data functions.
"""

# Import core functions

from .fetch_crime import fetch_and_store_crimes
from .fetch_weather import fetch_weather_for_all_locations
from .fetch_geocode import geocode_and_attach_locations
from .db_utils import set_up_database

# Define public exports 
__all__ = [
    "fetch_and_store_crimes",
    "fetch_weather_for_all_locations",
    "geocode_and_attach_locations",
    "set_up_database"
]
