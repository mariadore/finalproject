# src/config.py

import os

# Read API keys from environment; if missing, default to None
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
POSITIONSTACK_KEY = os.environ.get("POSITIONSTACK_KEY")

# Database filename (single database for project)
DB_FILENAME = "crime_weather.db"

# Limits
MAX_ITEMS_PER_RUN = 25  # required by project: <= 25 items per run
