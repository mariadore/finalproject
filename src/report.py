"""
Utilities for exporting calculated analysis results to a text file so the report
requirements are satisfied.
"""

from datetime import datetime
from pathlib import Path


def _describe_top_value(df, label_col, value_col):
    if df is None or df.empty or value_col not in df.columns:
        return "N/A"
    top = df.sort_values(value_col, ascending=False).iloc[0]
    label = top.get(label_col, "Unknown")
    value = top.get(value_col, 0)
    if isinstance(value, (int, float)):
        return f"{label}: {value:.2f}" if isinstance(value, float) else f"{label}: {value}"
    return f"{label}: {value}"


def write_analysis_report(path, df_weather, df_temp, df_types, df_wind=None, df_rain=None):
    """
    Persist a concise human-readable summary of the calculated data so it can be
    referenced in the written report.
    """
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    weather_types = (
        df_weather["weather_main"].nunique()
        if df_weather is not None and "weather_main" in df_weather.columns
        else 0
    )
    temp_obs = len(df_temp) if df_temp is not None else 0
    rain_types = (
        df_types["weather_main"].nunique()
        if df_types is not None and "weather_main" in df_types.columns
        else 0
    )

    lines = [
        "SI 201 Crime & Weather Analysis Summary",
        f"Generated: {datetime.utcnow():%Y-%m-%d %H:%M UTC}",
        "",
        "Weather Impact (avg crimes/day):",
        f"  Top weather condition → {_describe_top_value(df_weather, 'weather_main', 'avg_crimes_per_day')}",
        f"  Total weather types analyzed → {weather_types}",
        "",
        "Temperature Relationship:",
        f"  Observations analyzed → {temp_obs}",
        f"  Max crimes on a single day → {_describe_top_value(df_temp, 'weather_date', 'total_crimes')}",
        "",
        "Crime Type Distribution:",
        f"  Weather types represented → {rain_types}",
        f"  Dominant crime category → {_describe_top_value(df_types, 'category', 'crime_count')}",
        "",
        "Wind Effects:",
        f"  Highest crime wind bin → {_describe_top_value(df_wind, 'wind_bin', 'crime_count')}",
        "",
        "Precipitation Effects:",
        f"  Highest crime rain level → {_describe_top_value(df_rain, 'rain_level', 'crime_count')}",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")
