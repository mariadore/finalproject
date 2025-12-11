import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def detect_crime_column(df, preferred=None):
    """
    Detects the appropriate crime-related column in a dataframe.
    Returns the first matching column from a list of candidates.
    """
    candidates = [
        preferred,
        "avg_crimes_per_day",
        "total_crimes",
        "crime_count",
        "avg_crimes",
        "num_crimes"
    ]
    for col in candidates:
        if col is not None and col in df.columns:
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
    plt.ylim(0,50)
    plt.xticks(rotation=25)

    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2,
                 h + 0.3,
                 f"{h:.1f}",
                 ha="center",
                 fontsize=11)

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
    
    numeric_x=[]
    for b in df_temp["temp_bin"]:
        if "-" in b:
            low, high = b.reaplce("°C","").split("-")
            numeric_x.append((float(low)+float(high))/2)
        elif ">" in b:
            numeric_x.append(float(b.replace(">","").replace("°C","")) + 2)
        else:
            numeric_x.append(np.nan)
    df_temp["temp_numeric"] = numeric_x

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
    plt.xlin(0, max(x) +3)
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

    plt.legend(title="Crime Category",
               bbox_to_anchor=(1.05, 1),
               loc="upper left")

    plt.tight_layout()
    plt.savefig("crime_type_stacked.png")
    plt.close()
    print("Saved crime_type_stacked.png")



# Crimes Over Time (line chart)
def plot_crimes_over_time(df_weather):
    if df_weather.empty:
        print("df_weather is empty, skipping plot.")
        return

    value_col = detect_crime_column(df_weather)
    if value_col is None:
        return

    # If 'date' column is missing, create a synthetic one
    if "date" not in df_weather.columns:
        print("Warning: 'date' column missing, creating synthetic dates for plotting.")
        df_weather = df_weather.copy()
        df_weather["date"] = pd.date_range(start="2023-01-01", periods=len(df_weather))

    df = df_weather.sort_values("date")

    plt.figure(figsize=(12, 6))
    plt.plot(df["date"], df[value_col], marker="o")

    plt.title("Crimes Over Time", fontsize=16)
    plt.xlabel("Date", fontsize=14)
    plt.ylabel("Crime Count", fontsize=14)
    plt.xticks(rotation=25)

    plt.tight_layout()
    plt.savefig("crimes_over_time.png")
    plt.close()
    print("Saved crimes_over_time.png")


# Correlation Heatmap (crime + weather numerical variables)
def plot_correlation_heatmap(df_weather):
    if df_weather.empty:
        print("df_weather is empty, skipping heatmap.")
        return

    numeric_df = df_weather.select_dtypes(include=[np.number])
    if numeric_df.empty or numeric_df.shape[1] < 2:
        print("Not enough numeric columns for correlation heatmap.")
        return

    corr = numeric_df.corr()

    plt.figure(figsize=(12, 8))
    sns.heatmap(
        corr,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        linewidths=0.5,
        square=True,
        cbar_kws={"shrink": 0.8}
    )

    plt.title("Correlation Heatmap: Crime vs Weather Variables", fontsize=16)

    plt.tight_layout()
    plt.savefig("correlation_heatmap.png")
    plt.close()
    print("Saved correlation_heatmap.png")

# Main function to run all visualizations
def visualize_results(df_weather, df_temp, df_types):
    print("Creating visualizations...")

    # Required visualizations
    plot_avg_crimes_per_weather(df_weather)
    plot_crimes_vs_temperature(df_temp)
    plot_crime_type_distribution(df_types)

    # Extra visualizations (optional/bonus)
    plot_crimes_over_time(df_weather)
    plot_correlation_heatmap(df_weather)

    print("All visualizations saved!")
