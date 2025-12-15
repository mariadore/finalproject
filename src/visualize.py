import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update({
    "font.size": 12,
    "axes.titleweight": "bold",
    "axes.labelsize": 13,
    "axes.titlesize": 18
})

FIG_CAPTION = "Sources: UK Police, TomTom, Open-Meteo, TfL StopPoint."


def _finalize_figure(fig, filename, caption=FIG_CAPTION, show=False):
    """Apply shared padding + caption, then persist figure."""
    fig.tight_layout(rect=[0, 0.05, 1, 1])
    fig.text(0.01, 0.01, caption, fontsize=10, color="dimgray")
    fig.savefig(filename, dpi=300)
    if show:
        fig.show()
    else:
        plt.close(fig)


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


# Average crimes per weather
def plot_avg_crimes_per_weather(df_weather, show=False):
    if df_weather.empty:
        print("weather df empty → skipping")
        return

    col = detect_crime_column(df_weather, "avg_crimes_per_day")
    df = df_weather.sort_values(col, ascending=False).reset_index(drop=True)

    fig, ax1 = plt.subplots(figsize=(13, 7.5))
    colors = plt.cm.Blues(np.linspace(0.4, 0.95, len(df)))
    x = np.arange(len(df))
    bars = ax1.bar(x, df[col],
                   color=colors,
                   edgecolor="black",
                   linewidth=0.8)

    overall_mean = df[col].mean()
    ax1.axhline(overall_mean, linestyle="--", color="gray", linewidth=1.3, label=f"Overall Avg ({overall_mean:.1f})")

    max_bar = df[col].max() if not df[col].empty else 0
    ax1.set_ylim(0, max_bar * 1.35 + 0.05)
    ax1.set_title("Crime Rate by Weather Type (bar) + Total Crimes (line)", fontsize=18, weight="bold")
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
        value = bar.get_height()
        if value <= 0:
            continue
        offset = max(value * 0.02, 0.02)
        ax1.text(bar.get_x() + bar.get_width()/2,
                 value + offset,
                 f"{value:.1f}",
                 ha="center",
                 fontsize=11,
                 color="black")

    handles, labels = ax1.get_legend_handles_labels()
    if ax2:
        h2, l2 = ax2.get_legend_handles_labels()
        handles += h2
        labels += l2
    ax1.legend(
        handles,
        labels,
        loc="upper right",
        frameon=True,
        framealpha=0.9,
        edgecolor="dimgray"
    )

    _finalize_figure(fig, "avg_crimes_weather.png", show=show)


# Temerature vs. crime

def plot_crimes_vs_temperature(df_temp, show=False):
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
        fig, ax = plt.subplots(figsize=(13, 6))
        scatter = ax.scatter(x, y, s=sizes, c=y, cmap="coolwarm",
                             edgecolors="black", linewidth=0.4, alpha=0.75,
                             label="Location-Day Observations")
        cbar = fig.colorbar(scatter, ax=ax)
        cbar.set_label("Crimes per location-day", rotation=270, labelpad=15)

        if len(df) > 2:
            xs = np.linspace(x.min(), x.max(), 200)
            coeffs = np.polyfit(x, y, 2)
            trend = np.poly1d(coeffs)(xs)
            ax.plot(xs, trend, linestyle="--", color="dimgray", linewidth=2,
                    label="Quadratic Trend")

            window = max(5, len(df)//8)
            rolling = pd.Series(y).rolling(window=window, min_periods=1).mean()
            ax.plot(x, rolling, color="black", linewidth=2,
                    label=f"{window}-pt Rolling Avg")

        ax.set_xlabel("Average Temperature (°C)", fontsize=14)
        ax.set_title("Crimes vs Temperature (per location/day)", fontsize=18, weight="bold")
    else:
        # Fallback to bin-based display
        fig, ax = plt.subplots(figsize=(12, 6))
        x = np.arange(len(df_temp))
        y = df_temp[col]

        ax.scatter(x, y, s=200, c=y, cmap="coolwarm", edgecolors="black")

        if len(df_temp) > 1:
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            ax.plot(x, p(x), linestyle="--", color="gray")

        ax.set_xticks(x)
        ax.set_xticklabels(df_temp["temp_bin"], rotation=15)
        ax.set_xlabel("Temperature Bin (°C)", fontsize=14)
        ax.set_title("Crimes by Temperature Range", fontsize=18, weight="bold")

    ax.set_ylabel("Crimes per location-day", fontsize=14)

    ax.legend(loc="best", frameon=True, framealpha=0.9, edgecolor="dimgray")
    ax.grid(axis="both", linestyle=":", alpha=0.4)
    _finalize_figure(fig, "temp_vs_crime.png", show=show)


# Crime type distribution

def plot_crimes_vs_wind(df_wind, show=False):
    if df_wind is None or df_wind.empty:
        print("wind df empty → skipping")
        return

    col = detect_crime_column(df_wind, "crime_count")
    df = df_wind.copy()

    if "wind_bin" in df.columns:
        df["wind_bin"] = pd.to_numeric(df["wind_bin"], errors="coerce")
        df = df.dropna(subset=["wind_bin"]).sort_values("wind_bin")

    df["cumulative_pct"] = df[col].cumsum() / max(df[col].sum(), 1)

    fig, ax1 = plt.subplots(figsize=(12, 7))
    bars = ax1.bar(df["wind_bin"], df[col],
                   width=0.4,
                   color="#0f8a8a",
                   edgecolor="black",
                   alpha=0.8,
                   label="Crimes matched to weather day")
    ax1.set_xlabel("Wind Speed (m/s)", fontsize=14)
    ax1.set_ylabel("Crimes w/ same-day weather match", fontsize=14, color="#0f8a8a")
    ax1.tick_params(axis="y", colors="#0f8a8a")
    ax1.grid(axis="y", linestyle=":", alpha=0.4)

    ax1.set_ylim(0, df[col].max() * 1.3 + 1)

    ax2 = ax1.twinx()
    ax2.plot(df["wind_bin"], df["cumulative_pct"] * 100,
             color="darkred", marker="o", linewidth=2,
             label="Cumulative % of Crimes")
    ax2.set_ylabel("Cumulative %", fontsize=14, color="darkred")
    ax2.tick_params(axis="y", colors="darkred")
    ax2.set_ylim(0, 105)

    for bar in bars:
        height = bar.get_height()
        offset = max(height * 0.015, 0.2)
        ax1.text(bar.get_x() + bar.get_width()/2,
                 height + offset,
                 f"{int(height)}",
                 ha="center",
                 fontsize=10)

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc="upper left",
        frameon=True,
        framealpha=0.9,
        edgecolor="dimgray"
    )

    ax1.set_title("Crime Rate vs Wind Speed (+ cumulative share)", fontsize=18, weight="bold")
    _finalize_figure(fig, "wind_vs_crime.png", show=show)


def plot_precipitation_effect(df_rain, show=False):
    if df_rain is None or df_rain.empty:
        print("precip df empty → skipping")
        return

    col = detect_crime_column(df_rain, "crime_count")
    order = ["Dry", "Light Rain", "Heavy Rain"]
    df = df_rain.set_index("rain_level").reindex(order).fillna(0).reset_index()
    total = df[col].sum() or 1
    df["pct"] = (df[col] / total) * 100

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(df["rain_level"],
                   df[col],
                   color=["#f0c419", "#4aa3df", "#1f4e79"],
                   edgecolor="black",
                   alpha=0.9)

    for idx, bar in enumerate(bars):
        width = bar.get_width()
        pct = df.iloc[idx]["pct"]
        ax.text(width + max(width * 0.02, 0.5),
                bar.get_y() + bar.get_height()/2,
                f"{int(width)} ({pct:.1f}%)",
                va="center",
                fontsize=12)

    ax.set_title("Crime Rate by Rain Level", fontsize=18, weight="bold")
    ax.set_xlabel("Crimes w/ same-day weather match", fontsize=14)
    ax.set_ylabel("Rain Level", fontsize=14)
    ax.grid(axis="x", linestyle=":", alpha=0.4)

    _finalize_figure(fig, "precip_vs_crime.png", show=show)

def plot_transit_mode_crimes(df_transit, show=False):
    if df_transit is None or df_transit.empty:
        print("transit mode df empty → skipping")
        return

    df = df_transit.copy()
    df = df.sort_values("crime_count", ascending=False).head(10)
    if df.empty:
        print("transit mode df empty → skipping")
        return

    fig, ax1 = plt.subplots(figsize=(13.5, 6.5))
    colors = plt.cm.Purples(np.linspace(0.35, 0.9, len(df)))
    x = np.arange(len(df))
    bars = ax1.bar(x, df["crime_count"],
                   color=colors,
                   edgecolor="black",
                   linewidth=0.6,
                   label="Crimes within ~1 km of stop")
    ax1.set_ylabel("Crimes within ~1 km of stop", fontsize=14, color="#4a148c")
    ax1.tick_params(axis="y", colors="#4a148c")

    ax2 = ax1.twinx()
    ax2.plot(x, df["avg_crimes_per_stop"],
             color="#ff6f00",
             marker="o",
             linewidth=2,
             label="Avg nearby crimes per stop")
    ax2.set_ylabel("Avg nearby crimes per stop", fontsize=14, color="#ff6f00")
    ax2.tick_params(axis="y", colors="#ff6f00")

    for idx, bar in enumerate(bars):
        stop_count = int(df.iloc[idx]["stop_count"])
        crime_val = int(bar.get_height())
        avg_val = df.iloc[idx]["avg_crimes_per_stop"]
        ax2.text(idx,
                 avg_val + max(avg_val * 0.04, 0.05),
                 f"{crime_val} crimes\n{stop_count} stops",
                 ha="center",
                 va="bottom",
                 fontsize=9,
                 color="#333333")

    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc="upper right",
        frameon=True,
        framealpha=0.9,
        edgecolor="dimgray"
    )
    ax1.set_xlabel("Transit Modes", fontsize=14)
    ax1.set_xticks(x)
    ax1.set_xticklabels(df["mode"], rotation=20)
    ax1.set_title("Crimes Near TfL Transit Modes", fontsize=18, weight="bold")
    ax1.grid(axis="y", linestyle=":", alpha=0.4)
    _finalize_figure(fig, "transit_modes.png", show=show)

def plot_transit_hotspots(df_hotspots, show=False):
    if df_hotspots is None or df_hotspots.empty:
        print("transit hotspots empty → skipping")
        return

    df = df_hotspots.copy().head(15)
    df = df.sort_values("crime_count", ascending=True)
    labels = df["common_name"].fillna("Unknown Stop")
    colors = plt.cm.magma(np.linspace(0.3, 0.9, len(df)))

    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.barh(labels, df["crime_count"], color=colors, edgecolor="black")

    for idx, bar in enumerate(bars):
        modes = df.iloc[idx]["modes"] or df.iloc[idx]["stop_type"]
        ax.text(bar.get_width() + max(df["crime_count"].max() * 0.01, 1),
                bar.get_y() + bar.get_height()/2,
                f"{int(bar.get_width())} crimes\n{modes}",
                va="center",
                fontsize=10)

    ax.set_xlabel("Crimes within ~1 km", fontsize=14)
    ax.set_ylabel("TfL Stop", fontsize=14)
    ax.set_title("Top Transit Stops by Nearby Crimes", fontsize=18, weight="bold")
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    _finalize_figure(fig, "transit_hotspots.png", show=show)


# All visualizations 

def visualize_results(df_weather, df_temp, df_types, df_wind=None, df_rain=None,
                      df_transit=None, df_transit_hotspots=None, show_plots=False):
    print("Creating visualizations…")
    plot_avg_crimes_per_weather(df_weather, show=show_plots)
    plot_crimes_vs_temperature(df_temp, show=show_plots)
    plot_crimes_vs_wind(df_wind, show=show_plots)
    plot_precipitation_effect(df_rain, show=show_plots)
    plot_transit_mode_crimes(df_transit, show=show_plots)
    plot_transit_hotspots(df_transit_hotspots, show=show_plots)
    print("Visualizations saved!")
