import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

plt.style.use("seaborn-v0_8-whitegrid")


def detect_crime_column(df, preferred=None):
    cols = [
        preferred,
        "avg_crimes_per_day",
        "total_crimes",
        "crime_count"
    ]
    for c in cols:
        if c in df.columns:
            return c
    return None


# ---------------------------
# AVERAGE CRIMES PER WEATHER
# ---------------------------
def plot_avg_crimes_per_weather(df_weather):
    if df_weather.empty:
        print("weather df empty → skipping")
        return

    col = detect_crime_column(df_weather, "avg_crimes_per_day")
    df = df_weather.sort_values(col, ascending=False)

    plt.figure(figsize=(12, 6))
    bars = plt.bar(df["weather_main"], df[col],
                   color=plt.cm.Blues(np.linspace(0.4, 0.9, len(df))),
                   edgecolor="black")

    plt.title("Average Crimes per Weather Type", fontsize=18, weight="bold")
    plt.xlabel("Weather", fontsize=14)
    plt.ylabel("Avg Crimes per Day", fontsize=14)

    for bar in bars:
        plt.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() + 0.3,
                 f"{bar.get_height():.1f}",
                 ha="center",
                 fontsize=12)

    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig("avg_crimes_weather.png", dpi=300)
    plt.close()


# ---------------------------
# TEMPERATURE VS CRIME
# ---------------------------
def plot_crimes_vs_temperature(df_temp):
    if df_temp.empty:
        print("temperature df empty → skipping")
        return

    col = detect_crime_column(df_temp, "total_crimes")

    x = np.arange(len(df_temp))
    y = df_temp[col]

    plt.figure(figsize=(12, 6))
    plt.scatter(x, y, s=200, c=y, cmap="coolwarm", edgecolors="black")

    if len(df_temp) > 1:
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        plt.plot(x, p(x), linestyle="--", color="gray")

    plt.title("Crimes by Temperature Range", fontsize=18, weight="bold")
    plt.xlabel("Temperature Bin (°C)", fontsize=14)
    plt.ylabel("Total Crimes", fontsize=14)
    plt.xticks(x, df_temp["temp_bin"], rotation=15)

    plt.tight_layout()
    plt.savefig("temp_vs_crime.png", dpi=300)
    plt.close()


# ---------------------------
# CRIME TYPE DISTRIBUTION
# ---------------------------
def plot_crime_type_distribution(df_types):
    if df_types.empty:
        print("type df empty → skipping")
        return

    pivot = df_types.pivot_table(
        index="weather_main",
        columns="category",
        values="crime_count",
        aggfunc="sum",
        fill_value=0
    )

    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0)

    plt.figure(figsize=(14, 7))
    pivot_pct.plot(kind="bar",
                   stacked=True,
                   colormap="tab20",
                   ax=plt.gca())

    plt.title("Crime Type Distribution by Weather (%)", fontsize=18, weight="bold")
    plt.xlabel("Weather", fontsize=14)
    plt.ylabel("Percentage of Crimes", fontsize=14)

    plt.legend(title="Crime Category", bbox_to_anchor=(1.05, 1))
    plt.tight_layout()
    plt.savefig("crime_type_stacked.png", dpi=300)
    plt.close()


# ---------------------------
# ALL VISUALIZATIONS WRAPPER
# ---------------------------
def visualize_results(df_weather, df_temp, df_types):
    print("Creating visualizations…")
    plot_avg_crimes_per_weather(df_weather)
    plot_crimes_vs_temperature(df_temp)
    plot_crime_type_distribution(df_types)
    print("Visualizations saved!")
