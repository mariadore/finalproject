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
    df = df_weather.sort_values(col, ascending=False).reset_index(drop=True)

    fig, ax1 = plt.subplots(figsize=(13, 6))
    colors = plt.cm.Blues(np.linspace(0.4, 0.95, len(df)))
    x = np.arange(len(df))
    bars = ax1.bar(x, df[col],
                   color=colors,
                   edgecolor="black",
                   linewidth=0.8)

    overall_mean = df[col].mean()
    ax1.axhline(overall_mean, linestyle="--", color="gray", linewidth=1.3, label=f"Overall Avg ({overall_mean:.1f})")

    ax1.set_title("Average Crimes per Weather Type (bar) + Total Crimes (line)", fontsize=18, weight="bold")
    ax1.set_xlabel("Weather", fontsize=14)
    ax1.set_ylabel("Avg Crimes per Day", fontsize=14, color="midnightblue")
    ax1.tick_params(axis="y", colors="midnightblue")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["weather_main"], rotation=20)
    ax1.grid(axis="y", linestyle=":", alpha=0.4)

    if "total_crimes" in df.columns:
        ax2 = ax1.twinx()
        ax2.plot(x, df["total_crimes"],
                 color="crimson", marker="o", linewidth=2.2,
                 label="Total Crimes")
        ax2.set_ylabel("Total Crimes", fontsize=14, color="crimson")
        ax2.tick_params(axis="y", colors="crimson")
    else:
        ax2 = None

    for idx, bar in enumerate(bars):
        ax1.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() + 0.2,
                 f"{bar.get_height():.1f}",
                 ha="center",
                 fontsize=11,
                 color="black")

    handles, labels = ax1.get_legend_handles_labels()
    if ax2:
        h2, l2 = ax2.get_legend_handles_labels()
        handles += h2
        labels += l2
    ax1.legend(handles, labels, loc="upper right", frameon=True)

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

    if "temp_c" in df_temp.columns:
        df = df_temp.dropna(subset=["temp_c"]).sort_values("temp_c")
        if df.empty:
            print("temperature df missing temp_c → skipping")
            return

        x = df["temp_c"].values
        y = df[col].values

        sizes = np.interp(y, (y.min(), y.max()), (60, 400)) if y.max() != y.min() else np.full_like(y, 120)
        plt.figure(figsize=(13, 6))
        scatter = plt.scatter(x, y, s=sizes, c=y, cmap="coolwarm",
                              edgecolors="black", linewidth=0.4, alpha=0.75,
                              label="Location-Day Observations")
        cbar = plt.colorbar(scatter)
        cbar.set_label("Crime Count", rotation=270, labelpad=15)

        if len(df) > 2:
            xs = np.linspace(x.min(), x.max(), 200)
            coeffs = np.polyfit(x, y, 2)
            trend = np.poly1d(coeffs)(xs)
            plt.plot(xs, trend, linestyle="--", color="dimgray", linewidth=2,
                     label="Quadratic Trend")

            window = max(5, len(df)//8)
            rolling = pd.Series(y).rolling(window=window, min_periods=1).mean()
            plt.plot(x, rolling, color="black", linewidth=2,
                     label=f"{window}-pt Rolling Avg")

        plt.xlabel("Average Temperature (°C)", fontsize=14)
        plt.title("Crimes vs Temperature (per location/day)", fontsize=18, weight="bold")
    else:
        # Fallback to bin-based display
        plt.figure(figsize=(12, 6))
        x = np.arange(len(df_temp))
        y = df_temp[col]

        plt.scatter(x, y, s=200, c=y, cmap="coolwarm", edgecolors="black")

        if len(df_temp) > 1:
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            plt.plot(x, p(x), linestyle="--", color="gray")

        plt.xticks(x, df_temp["temp_bin"], rotation=15)
        plt.xlabel("Temperature Bin (°C)", fontsize=14)
        plt.title("Crimes by Temperature Range", fontsize=18, weight="bold")

    plt.ylabel("Total Crimes", fontsize=14)

    plt.legend(loc="best")
    plt.grid(axis="both", linestyle=":", alpha=0.4)
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

    if pivot.shape[1] <= 1:
        # Only a single category present → show raw counts instead of a flat 100% stack.
        plt.figure(figsize=(12, 6))
        series = pivot.iloc[:, 0]
        plt.bar(series.index, series.values, color="steelblue", edgecolor="black")
        plt.title("Crime Counts by Weather", fontsize=18, weight="bold")
        plt.xlabel("Weather", fontsize=14)
        plt.ylabel("Total Crimes", fontsize=14)
        plt.xticks(rotation=20)
        plt.tight_layout()
        plt.savefig("crime_type_stacked.png", dpi=300)
        plt.close()
        return

    totals = pivot.sum(axis=1)
    pivot_pct = pivot.div(totals, axis=0)

    fig, ax1 = plt.subplots(figsize=(14, 8))
    x = np.arange(len(pivot_pct))
    bottom = np.zeros(len(pivot_pct))
    colors = plt.cm.tab20(np.linspace(0, 1, len(pivot_pct.columns)))

    for idx, category in enumerate(pivot_pct.columns):
        vals = pivot_pct[category].values
        ax1.bar(x,
                vals * 100,
                bottom=bottom * 100,
                label=category,
                color=colors[idx],
                edgecolor="white")
        bottom += vals

    ax1.set_xticks(x)
    ax1.set_xticklabels(pivot_pct.index, rotation=20)
    ax1.set_ylabel("Percentage of Crimes (%)", fontsize=14)
    ax1.set_xlabel("Weather", fontsize=14)
    ax1.set_title("Crime Type Distribution by Weather (stacked %) + Totals (line)", fontsize=18, weight="bold")
    ax1.set_ylim(0, 105)

    ax2 = ax1.twinx()
    ax2.plot(x, totals.values, color="black", linewidth=2.2, marker="o", label="Total Crimes")
    ax2.set_ylabel("Total Crimes", fontsize=14)

    for idx, total in enumerate(totals.values):
        ax2.text(x[idx], total + max(total * 0.02, 1),
                 f"{int(total)}",
                 ha="center",
                 va="bottom",
                 fontsize=11,
                 color="black")

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2,
               title="Crime Categories",
               bbox_to_anchor=(1.02, 1),
               loc="upper left",
               frameon=True)

    plt.tight_layout()
    plt.savefig("crime_type_stacked.png", dpi=300)
    plt.close()


def plot_crimes_vs_wind(df_wind):
    if df_wind is None or df_wind.empty:
        print("wind df empty → skipping")
        return

    col = detect_crime_column(df_wind, "crime_count")
    df = df_wind.copy()

    if "wind_bin" in df.columns:
        df["wind_bin"] = pd.to_numeric(df["wind_bin"], errors="coerce")
        df = df.dropna(subset=["wind_bin"]).sort_values("wind_bin")

    df["cumulative_pct"] = df[col].cumsum() / max(df[col].sum(), 1)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    bars = ax1.bar(df["wind_bin"], df[col],
                   width=0.4,
                   color="#0f8a8a",
                   edgecolor="black",
                   alpha=0.8,
                   label="Crime Count")
    ax1.set_xlabel("Wind Speed (m/s)", fontsize=14)
    ax1.set_ylabel("Total Crimes", fontsize=14, color="#0f8a8a")
    ax1.tick_params(axis="y", colors="#0f8a8a")
    ax1.grid(axis="y", linestyle=":", alpha=0.4)

    ax2 = ax1.twinx()
    ax2.plot(df["wind_bin"], df["cumulative_pct"] * 100,
             color="darkred", marker="o", linewidth=2,
             label="Cumulative % of Crimes")
    ax2.set_ylabel("Cumulative %", fontsize=14, color="darkred")
    ax2.tick_params(axis="y", colors="darkred")
    ax2.set_ylim(0, 105)

    for bar in bars:
        ax1.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() + max(bar.get_height() * 0.02, 0.5),
                 f"{int(bar.get_height())}",
                 ha="center",
                 fontsize=10)

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, loc="upper left")

    plt.title("Crime Counts vs Wind Speed (+ cumulative share)", fontsize=18, weight="bold")
    plt.tight_layout()
    plt.savefig("wind_vs_crime.png", dpi=300)
    plt.close()


def plot_precipitation_effect(df_rain):
    if df_rain is None or df_rain.empty:
        print("precip df empty → skipping")
        return

    col = detect_crime_column(df_rain, "crime_count")
    order = ["Dry", "Light Rain", "Heavy Rain"]
    df = df_rain.set_index("rain_level").reindex(order).fillna(0).reset_index()
    total = df[col].sum() or 1
    df["pct"] = (df[col] / total) * 100

    plt.figure(figsize=(11, 5))
    bars = plt.barh(df["rain_level"],
                    df[col],
                    color=["#f0c419", "#4aa3df", "#1f4e79"],
                    edgecolor="black",
                    alpha=0.9)

    for idx, bar in enumerate(bars):
        width = bar.get_width()
        pct = df.iloc[idx]["pct"]
        plt.text(width + max(total * 0.01, 1),
                 bar.get_y() + bar.get_height()/2,
                 f"{int(width)} ({pct:.1f}%)",
                 va="center",
                 fontsize=12)

    plt.title("Crime Counts by Rain Level", fontsize=18, weight="bold")
    plt.xlabel("Total Crimes", fontsize=14)
    plt.ylabel("Rain Level", fontsize=14)
    plt.grid(axis="x", linestyle=":", alpha=0.4)

    plt.tight_layout()
    plt.savefig("precip_vs_crime.png", dpi=300)
    plt.close()

def plot_crimes_vs_transit(df_transit):
    if df_transit is None or df_transit.empty:
        print("transit df empty → skipping")
        return

    df = df_transit.copy().sort_values("crime_count", ascending=False).reset_index(drop=True)
    x = np.arange(len(df))
    colors = plt.cm.Purples(np.linspace(0.4, 0.9, len(df)))

    fig, ax1 = plt.subplots(figsize=(12, 6))
    bars = ax1.bar(x,
                   df["crime_count"],
                   color=colors,
                   edgecolor="black",
                   width=0.6,
                   label="Total Crimes")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["mode"])
    ax1.set_xlabel("TfL Mode", fontsize=14)
    ax1.set_ylabel("Total Crimes (within ~1 km)", fontsize=14, color="#4b2c7f")
    ax1.tick_params(axis="y", colors="#4b2c7f")
    ax1.grid(axis="y", linestyle=":", alpha=0.4)

    if "avg_crimes_per_stop" in df.columns:
        ax2 = ax1.twinx()
        ax2.plot(x,
                 df["avg_crimes_per_stop"],
                 color="#f39c12",
                 marker="o",
                 linewidth=2.2,
                 label="Avg Crimes per Stop")
        ax2.set_ylabel("Avg Crimes per Stop", fontsize=14, color="#f39c12")
        ax2.tick_params(axis="y", colors="#f39c12")
    else:
        ax2 = None

    for idx, bar in enumerate(bars):
        text = f"{int(bar.get_height())}"
        if "stop_count" in df.columns:
            text += f"\n({int(df.iloc[idx]['stop_count'])} stops)"
        ax1.text(bar.get_x() + bar.get_width()/2,
                 bar.get_height() + max(bar.get_height() * 0.02, 0.5),
                 text,
                 ha="center",
                 va="bottom",
                 fontsize=11)

    handles, labels = ax1.get_legend_handles_labels()
    if ax2:
        h2, l2 = ax2.get_legend_handles_labels()
        handles += h2
        labels += l2
    ax1.legend(handles, labels, loc="upper right", frameon=True)

    plt.title("Crimes Near Transit Stops by Mode", fontsize=18, weight="bold")
    plt.tight_layout()
    plt.savefig("transit_vs_crime.png", dpi=300)
    plt.close()


# All visualizations 

def visualize_results(df_weather, df_temp, df_types, df_wind=None, df_rain=None, df_transit=None):
    print("Creating visualizations…")
    plot_avg_crimes_per_weather(df_weather)
    plot_crimes_vs_temperature(df_temp)
    plot_crime_type_distribution(df_types)
    plot_crimes_vs_wind(df_wind)
    plot_precipitation_effect(df_rain)
    plot_crimes_vs_transit(df_transit)
    print("Visualizations saved!")
