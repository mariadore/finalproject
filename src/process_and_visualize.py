import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def load_joined_data(conn):
    """
    Join CrimeData → LocationData → WeatherData
    Match by:
      - Crime.month (YYYY-MM)
      - Weather.date (YYYY-MM-DD) substring
      - LocationData.location_id
    """

    query = """
        SELECT 
            c.crime_id,
            c.category AS crime_type,
            c.month,
            l.city,
            w.date,
            w.temp_c,
            w.weather_main
        FROM CrimeData c
        JOIN LocationData l ON c.location_id = l.location_id
        JOIN WeatherData w ON w.location_id = l.location_id
             AND substr(w.date,1,7) = c.month
    """

    return pd.read_sql_query(query, conn)


def plot_avg_crimes_by_weather(df):
    grp = df.groupby(["date", "weather_main"]).size().reset_index(name="crimes")
    avg = grp.groupby("weather_main")["crimes"].mean()

    avg.plot(kind="bar")
    plt.title("Average Crimes per Weather Type")
    plt.xlabel("Weather")
    plt.ylabel("Average Crimes per Day")
    plt.tight_layout()
    plt.savefig("avg_crimes_weather.png")
    plt.clf()


def plot_temp_vs_crime(df):
    crimes_per_day = df.groupby("date").size().reset_index(name="crime_count")
    temps = df.groupby("date")["temp_c"].mean().reset_index()

    merged = crimes_per_day.merge(temps, on="date")

    plt.scatter(merged["temp_c"], merged["crime_count"])
    plt.xlabel("Temperature (C)")
    plt.ylabel("Crimes")
    plt.title("Temperature vs Crime")
    plt.tight_layout()
    plt.savefig("temp_vs_crime.png")
    plt.clf()


def plot_crime_type_stacked(df):
    pivot = pd.crosstab(df["weather_main"], df["crime_type"])
    pivot.plot(kind="bar", stacked=True)
    plt.title("Crime Types by Weather")
    plt.xlabel("Weather")
    plt.ylabel("Crime Count")
    plt.tight_layout()
    plt.savefig("crime_type_stacked.png")
    plt.clf()


def plot_crime_over_time(df):
    df["date"]=pd.to_datetime(df["date"])
    crimes_by_day=df.groupby("date").size()
    crimes_by_day.plot(kind="line")
    plt.title("Crimes Over Time")
    plt.xlabel("Date")
    plt.ylabel("Number of Crimes")
    plt.tight_layout()
    plt.savefig("crimes_over_time.png")
    plt.clf()


def run_visualizations(conn):
    df = load_joined_data(conn)
    if df.empty:
        print("No joined data found. Cannot visualize.")
        return

    plot_avg_crimes_by_weather(df)
    plot_temp_vs_crime(df)
    plot_crime_type_stacked(df)
    plot_crime_over_time(df)

    print("Visualization PNGs saved.")

