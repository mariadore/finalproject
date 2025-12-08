import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def detect_crime_column(df, preferred=None):
    candidates = [
        preferred,
        "avg_crimes_per_day",
        "total_crimes",
        "crime_count",
        "avg_crimes",
        "num_crimes"
    ]
    for col in candidates:
        if col in df.columns and col is not None:
            return col
    print("ERROR: No usable crime column found in dataframe.")
    print("Available columns:", df.columns)
    return None


# Average Crimes per Weather
def plot_avg_crimes_per_weather(df_weather):
    if df_weather.empty:
        print("df_weather is empty, skipping plot.")
        return

    value_col = detect_crime_column(df_weather, preferred="avg_crimes_per_day")
    if value_col is None:
        return

    df = df_weather.sort_values(value_col, ascending=False)

    plt.figure(figsize=(10, 6))
    colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(df)))

    bars = plt.bar(df["weather_main"], df[value_col], color=colors)

    plt.title("Average Crimes per Weather Type", fontsize=16)
    plt.xlabel("Weather", fontsize=14)
    plt.ylabel("Average Crimes per Day", fontsize=14)
    plt.xticks(rotation=25)

    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, h + 0.3,
                 f"{h:.1f}", ha="center", fontsize=11)

    plt.tight_layout()
    plt.savefig("avg_crimes_weather.png")
    plt.close()
    print("Saved avg_crimes_weather.png")


# Crimes vs Temperature Range + Trendline
def plot_crimes_vs_temperature(df_temp):
    if df_temp.empty:
        print("df_temp is empty, skipping plot.")
        return

    value_col = detect_crime_column(df_temp, preferred="total_crimes")
    if value_col is None:
        return

    x = np.arange(len(df_temp))
    y = df_temp[value_col]

    plt.figure(figsize=(10, 6))
    plt.scatter(x, y, s=200, c=y, cmap="coolwarm", edgecolors="black")

    if len(df_temp) >= 2:
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        plt.plot(x, p(x), linestyle="--", color="gray", linewidth=2)

    plt.title("Crimes vs Temperature Range", fontsize=16)
    plt.xlabel("Temperature Bin", fontsize=14)
    plt.ylabel("Total Crimes", fontsize=14)
    plt.xticks(x, df_temp["temp_bin"], rotation=15)
    plt.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig("temp_vs_crime.png")
    plt.close()
    print("Saved temp_vs_crime.png")


# Crime Type Distribution by Weather (percent stacked)
def plot_crime_type_distribution(df_types):
    if df_types.empty:
        print("df_types is empty, skipping plot.")
        return

    value_col = detect_crime_column(df_types, preferred="crime_count")
    if value_col is None:
        return

    pivot = df_types.pivot_table(
        index="weather_main",
        columns="category",
        values=value_col,
        aggfunc="sum",
        fill_value=0
    )

    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0)

    plt.figure(figsize=(12, 7))
    pivot_pct.plot(kind="bar",
                   stacked=True,
                   colormap="tab20",
                   figsize=(12, 7))

    plt.title("Crime Type Distribution by Weather (Percent)", fontsize=16)
    plt.xlabel("Weather", fontsize=14)
    plt.ylabel("Percent of Crimes", fontsize=14)
    plt.legend(title="Crime Category", bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()
    plt.savefig("crime_type_stacked.png")
    plt.close()
    print("Saved crime_type_stacked.png")


def visualize_results(df_weather, df_temp, df_types):
    print("Creating visualizations...")

    plot_avg_crimes_per_weather(df_weather)
    plot_crimes_vs_temperature(df_temp)
    plot_crime_type_distribution(df_types)

    print("All visualizations saved!")
