import matplotlib.pyplot as plt
import pandas as pd



def plot_crimes_by_weather(df):
    if df.empty:
        print("No weather data to plot.")
        return

    plt.figure(figsize=(8, 5))
    plt.bar(df["weather_main"], df["avg_crimes_per_day"], color="skyblue", edgecolor="black")

    plt.title("Average Crimes per Weather Type")
    plt.xlabel("Weather")
    plt.ylabel("Average Crimes per Day")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("avg_crimes_weather.png")
    plt.close()



def plot_temp_vs_crime(df):
    if df.empty:
        print("No temperature data to plot.")
        return

    plt.figure(figsize=(8, 5))
    plt.scatter(df["temp_bin"], df["total_crimes"], s=80)

    plt.title("Crimes vs Temperature Range")
    plt.xlabel("Temperature Bin")
    plt.ylabel("Total Crimes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("temp_vs_crime.png")
    plt.close()




def plot_crime_type_distribution(df):
    if df.empty:
        print("No crime type data to plot.")
        return

    pivot_df = df.pivot(index="weather_main", columns="category", values="crime_count").fillna(0)

    plt.figure(figsize=(10, 6))
    pivot_df.plot(kind="bar", stacked=True, colormap="tab20", figsize=(10, 6))

    plt.title("Crime Type Distribution by Weather")
    plt.xlabel("Weather")
    plt.ylabel("Crime Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("crime_type_stacked.png")
    plt.close()



def visualize_results(df_weather, df_temp, df_types):
    print("Plotting crime data...")

    plot_crimes_by_weather(df_weather)
    plot_temp_vs_crime(df_temp)
    plot_crime_type_distribution(df_types)

    print("All charts saved.")
