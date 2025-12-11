# etl_analysis.py
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("[ERROR] Missing SUPABASE_URL or SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"


def load_data_from_supabase() -> pd.DataFrame:
    print("[FETCHING] Fetching air quality data from Supabase...")
    data = supabase.table(TABLE_NAME).select("*").execute()

    if hasattr(data, "data"):
        df = pd.DataFrame(data.data)
    else:
        df = pd.DataFrame(data)

    if df.empty:
        raise SystemExit("[ERROR] No data found in Supabase table.")

    # Convert timestamps
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def compute_kpi_metrics(df: pd.DataFrame):
    print("[KPI] Computing KPI Metrics...")

    # City with highest avg PM2.5
    highest_pm25_city = (
        df.groupby("city")["pm2_5"].mean().idxmax()
    )
    highest_pm25_value = df.groupby("city")["pm2_5"].mean().max()

    # City with highest average severity score
    highest_severity_city = (
        df.groupby("city")["severity_score"].mean().idxmax()
    )
    highest_severity_value = df.groupby("city")["severity_score"].mean().max()

    # Risk distribution
    risk_dist = (
        df["risk_flag"].value_counts(normalize=True) * 100
    ).round(2)

    # Hour with worst AQI (based on severity score)
    worst_hour = (
        df.groupby("hour")["severity_score"].mean().idxmax()
    )
    worst_hour_value = df.groupby("hour")["severity_score"].mean().max()

    summary_df = pd.DataFrame({
        "metric": [
            "City with highest PM2.5",
            "Highest PM2.5 value",
            "City with highest severity",
            "Highest severity value",
            "Worst hour of day (severity)",
            "Worst hour severity value"
        ],
        "value": [
            highest_pm25_city,
            highest_pm25_value,
            highest_severity_city,
            highest_severity_value,
            worst_hour,
            worst_hour_value
        ]
    })

    return summary_df, risk_dist


def compute_pollution_trends(df: pd.DataFrame):
    print("[TRENDS] Building pollution trends dataset...")

    trends = df[["city", "time", "pm2_5", "pm10", "ozone"]].sort_values("time")
    return trends


def save_outputs(summary_df, risk_dist, trends_df):
    print("[SAVING] Saving CSV outputs...")

    # Summary metrics
    summary_df.to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)

    # Risk distribution
    risk_dist_df = risk_dist.reset_index()
    risk_dist_df.columns = ["risk_flag", "percentage"]
    risk_dist_df.to_csv(PROCESSED_DIR / "city_risk_distribution.csv", index=False)

    # Pollution trends
    trends_df.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)

    print("[SUCCESS] CSV files saved.")


def save_visualizations(df):
    print("[PLOTTING] Generating visualizations...")

    sns.set(style="whitegrid")

    # 1. Histogram PM2.5
    plt.figure(figsize=(8, 5))
    sns.histplot(df["pm2_5"], bins=30, kde=True)
    plt.title("Histogram of PM2.5")
    plt.savefig(PROCESSED_DIR / "hist_pm25.png")
    plt.close()

    # 2. Bar chart of risk per city
    plt.figure(figsize=(10, 6))
    sns.countplot(data=df, x="city", hue="risk_flag")
    plt.title("Risk Levels per City")
    plt.xticks(rotation=45)
    plt.savefig(PROCESSED_DIR / "risk_flags_per_city.png")
    plt.close()

    # 3. Line chart of hourly PM2.5 trends
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x="time", y="pm2_5", hue="city")
    plt.title("Hourly PM2.5 Trends")
    plt.savefig(PROCESSED_DIR / "pm25_trends.png")
    plt.close()

    # 4. Scatter severity vs PM2.5
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df, x="pm2_5", y="severity_score", hue="city")
    plt.title("Severity Score vs PM2.5")
    plt.savefig(PROCESSED_DIR / "severity_vs_pm25.png")
    plt.close()

    print("[SUCCESS] PNG plots saved.")



if __name__ == "__main__":
    df = load_data_from_supabase()

    summary_df, risk_distribution = compute_kpi_metrics(df)
    trends_df = compute_pollution_trends(df)

    save_outputs(summary_df, risk_distribution, trends_df)

    save_visualizations(df)

    print("[COMPLETE] ETL Analysis Complete!")