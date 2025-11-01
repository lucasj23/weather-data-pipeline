import os, json
import pandas as pd
from transformations.clean_weather import clean_weather

def _write_raw(tmp_path, run_date, payload): # carpeta temporal
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw" / run_date
    raw_dir.mkdir(parents=True, exist_ok=True)
    with open(raw_dir / "weather.json", "w") as f:
        json.dump(payload, f)
    return str(data_dir)

def test_clean_weather_basic(tmp_path, monkeypatch):
    run_date = "2025-01-15"
    payload = {
        "run_date": run_date,
        "data": [{
            "_city_code": "BUE",
            "daily": {
                "time": ["2025-01-10","2025-01-11"],
                "temperature_2m_max": [30.0, 28.0],
                "temperature_2m_min": [20.0, 18.0],
                "precipitation_sum": [0.0, 5.2],
            }
        }]
    }
    data_dir = _write_raw(tmp_path, run_date, payload)
    monkeypatch.setenv("DATA_DIR", data_dir)

    out_parquet = clean_weather(run_date=run_date)
    assert os.path.exists(out_parquet), "Clean parquet was not created"

    df = pd.read_parquet(out_parquet)
    # Min columns expectedf
    expected = {"run_date","city_code","date","temp_max","temp_min","precip_mm","temp_avg","temp_range"}
    assert expected.issubset(df.columns), f"Missing columns: {expected - set(df.columns)}"

    # Calculations: avgs and ranges
    df = df.sort_values("date").reset_index(drop=True)
    assert df.loc[0, "temp_avg"] == (30.0 + 20.0)/2
    assert df.loc[0, "temp_range"] == 30.0 - 20.0
    assert df.loc[1, "temp_avg"] == (28.0 + 18.0)/2
    assert df.loc[1, "temp_range"] == 28.0 - 18.0

def test_clean_weather_empty_raw(tmp_path, monkeypatch):
    run_date = "2025-01-16"
    payload = {"run_date": run_date, "data": []}
    data_dir = _write_raw(tmp_path, run_date, payload)
    monkeypatch.setenv("DATA_DIR", data_dir)

    out_parquet = clean_weather(run_date=run_date)
    assert os.path.exists(out_parquet), "Clean parquet should exist even with empty RAW"
    df = pd.read_parquet(out_parquet)
    assert df.empty, "Parquet should be empty when RAW has no data"