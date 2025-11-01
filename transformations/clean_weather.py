import glob
import json
import os

import pandas as pd


def _latest_run_date(base_dir: str) -> str:
    parts = sorted(glob.glob(os.path.join(base_dir, "raw", "*")))
    if not parts:
        raise FileNotFoundError("No RAW partitions found")
    return os.path.basename(parts[-1])


def clean_weather(run_date: str | None = None) -> str:
    BASE_DIR = os.getenv("DATA_DIR", "./data")
    """Read RAW Open-Meteo data and create a tidy table: one row per city-date."""
    run_date = run_date or _latest_run_date(BASE_DIR)
    raw_path = os.path.join(BASE_DIR, "raw", run_date, "weather.json")

    # Load the JSON payload
    with open(raw_path, "r") as f:
        payload = json.load(f)

    rows = []
    # Flatten JSON: each city blob contains daily weather arrays
    for city_blob in payload.get("data", []):
        city = city_blob.get("_city_code")
        daily = city_blob.get("daily", {})
        dates = daily.get("time", [])
        tmax = daily.get("temperature_2m_max", [])
        tmin = daily.get("temperature_2m_min", [])
        prcp = daily.get("precipitation_sum", [])
        # Build one record per date
        for d, mx, mn, pc in zip(dates, tmax, tmin, prcp):
            rows.append(
                {
                    "run_date": run_date,
                    "city_code": city,
                    "date": d,
                    "temp_max": mx,
                    "temp_min": mn,
                    "precip_mm": pc,
                }
            )

    df = pd.DataFrame(rows)

    # Compute derived metrics
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["temp_avg"] = (df["temp_max"] + df["temp_min"]) / 2
        df["temp_range"] = df["temp_max"] - df["temp_min"]

    # Save clean (silver zone)
    out_dir = os.path.join(BASE_DIR, "clean", run_date)
    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, "weather.parquet")
    df.to_parquet(out_parquet, index=False)

    # Export a CSV sample for quick inspection (this is just to check the data and analyze columns)
    df.head(200).to_csv(os.path.join(out_dir, "weather_sample.csv"), index=False)

    print(f"✅ CLEAN saved: {out_parquet}  rows={len(df)}")
    return out_parquet


if __name__ == "__main__":
    clean_weather()
