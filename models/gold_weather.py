import datetime
import glob
import os

import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "./data")


def build_gold(run_date: str | None = None) -> str:
    """
    GOLD layer builder:
      - Reads all SILVER (clean) partitions (data/clean/*/weather.parquet)
      - Builds daily KPIs (with 7d/14d rolling) and YoY same-day deltas
      - Builds monthly aggregates
      - Saves GOLD outputs partitioned by run_date
    """
    # 1) Read all clean partitions
    clean_paths = glob.glob(os.path.join(DATA_DIR, "clean", "*", "weather.parquet"))
    if not clean_paths:
        print("No clean (silver) files found.")
        return ""
    df = pd.concat([pd.read_parquet(p) for p in clean_paths], ignore_index=True)

    # Basic validations
    needed = {
        "run_date",
        "city_code",
        "date",
        "temp_max",
        "temp_min",
        "temp_avg",
        "temp_range",
        "precip_mm",
    }
    missing = needed - set(df.columns)
    if missing:
        print(f"Missing required columns for GOLD: {missing}")
        return ""

    # 2) Ensure dtypes and helpers
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["yyyymm"] = df["date"].dt.to_period("M").astype(str)

    # --------------------
    # DAILY KPIs (city-date)
    # --------------------
    daily_kpi = (
        df.groupby(["city_code", "date"], dropna=False)
        .agg(
            temp_max=("temp_max", "max"),
            temp_min=("temp_min", "min"),
            temp_avg=("temp_avg", "mean"),
            temp_range=("temp_range", "mean"),
            precip_mm=("precip_mm", "sum"),
        )
        .reset_index()
        .sort_values(["city_code", "date"])
    )

    # 2.a Rolling 7d / 14d (by city)
    daily_kpi["avg_max_7d"] = daily_kpi.groupby("city_code")["temp_max"].transform(
        lambda s: s.rolling(7, min_periods=1).mean()
    )
    daily_kpi["avg_min_7d"] = daily_kpi.groupby("city_code")["temp_min"].transform(
        lambda s: s.rolling(7, min_periods=1).mean()
    )
    daily_kpi["avg_max_14d"] = daily_kpi.groupby("city_code")["temp_max"].transform(
        lambda s: s.rolling(14, min_periods=1).mean()
    )
    daily_kpi["avg_min_14d"] = daily_kpi.groupby("city_code")["temp_min"].transform(
        lambda s: s.rolling(14, min_periods=1).mean()
    )

    # 2.b YoY same-day (shift by 1 year)
    prev = daily_kpi[["city_code", "date", "temp_min", "temp_max"]].copy()
    # Mapeamos el dato del año pasado al mismo día de este año:
    prev["date"] = prev["date"] + pd.DateOffset(years=1)
    prev = prev.rename(columns={"temp_min": "temp_min_ly", "temp_max": "temp_max_ly"})

    daily_kpi = daily_kpi.merge(prev, on=["city_code", "date"], how="left")

    # Variaciones % (evitar div/0)
    def _pct(curr, prev):
        return (curr - prev) / prev.replace({0: pd.NA}) * 100

    if "temp_min_ly" in daily_kpi.columns:
        daily_kpi["temp_min_yoy_pct"] = _pct(
            daily_kpi["temp_min"], daily_kpi["temp_min_ly"]
        )
    if "temp_max_ly" in daily_kpi.columns:
        daily_kpi["temp_max_yoy_pct"] = _pct(
            daily_kpi["temp_max"], daily_kpi["temp_max_ly"]
        )

    # --------------------
    # MONTHLY KPIs (city-month)
    # --------------------
    monthly_kpi = (
        df.groupby(["city_code", "yyyymm"], dropna=False)
        .agg(
            days=("date", "nunique"),
            avg_temp=("temp_avg", "mean"),
            max_temp=("temp_max", "max"),
            min_temp=("temp_min", "min"),
            avg_range=("temp_range", "mean"),
            total_precip_mm=("precip_mm", "sum"),
            rainy_days=("precip_mm", lambda s: (s > 0).sum()),
        )
        .reset_index()
        .sort_values(["city_code", "yyyymm"])
    )

    # 3-month rolling average temperature (by city)
    monthly_kpi["avg_temp_roll3"] = monthly_kpi.groupby("city_code")[
        "avg_temp"
    ].transform(lambda s: s.rolling(3, min_periods=1).mean())

    # --- Round numeric metrics to 1 decimal ---
    for col in [
        "temp_max",
        "temp_min",
        "temp_avg",
        "temp_range",
        "precip_mm",
        "avg_max_7d",
        "avg_min_7d",
        "avg_max_14d",
        "avg_min_14d",
        "temp_min_ly",
        "temp_max_ly",
        "temp_min_yoy_pct",
        "temp_max_yoy_pct",
    ]:
        if col in daily_kpi.columns:
            daily_kpi[col] = daily_kpi[col].round(1)

    for col in [
        "avg_temp",
        "max_temp",
        "min_temp",
        "avg_range",
        "total_precip_mm",
        "avg_temp_roll3",
    ]:
        if col in monthly_kpi.columns:
            monthly_kpi[col] = monthly_kpi[col].round(1)

    # --------------------
    # Save GOLD
    # --------------------
    run_date = run_date or datetime.date.today().isoformat()
    out_dir = os.path.join(DATA_DIR, "gold", run_date)
    os.makedirs(out_dir, exist_ok=True)

    daily_parquet = os.path.join(out_dir, "weather_daily_kpis.parquet")
    monthly_parquet = os.path.join(out_dir, "weather_monthly_kpis.parquet")
    daily_enriched_parquet = os.path.join(out_dir, "weather_daily_enriched.parquet")

    daily_kpi.to_parquet(daily_enriched_parquet, index=False)  # con rolling + YoY
    # (si querés mantener el daily "simple", lo podés guardar también)
    daily_kpi[
        [
            "city_code",
            "date",
            "temp_max",
            "temp_min",
            "temp_avg",
            "temp_range",
            "precip_mm",
        ]
    ].to_parquet(daily_parquet, index=False)
    monthly_kpi.to_parquet(monthly_parquet, index=False)

    # CSV samples
    daily_kpi.head(200).to_csv(
        os.path.join(out_dir, "weather_daily_enriched_sample.csv"), index=False
    )
    monthly_kpi.head(200).to_csv(
        os.path.join(out_dir, "weather_monthly_kpis_sample.csv"), index=False
    )

    print(
        f"✅ GOLD saved:\n  {daily_parquet}\n  {daily_enriched_parquet}\n  {monthly_parquet}"
    )
    return out_dir


if __name__ == "__main__":
    build_gold()
