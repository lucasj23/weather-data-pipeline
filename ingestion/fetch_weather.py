import json
import os
from datetime import date, timedelta
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_city_weather(
    city_code: str, lat: float, lon: float, start_date: str, end_date: str
) -> dict:
    """Fetch daily weather for a city and attach metadata for the pipeline."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto",
        "start_date": start_date,
        "end_date": end_date,
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    payload["_city_code"] = city_code
    payload["_start_date"] = start_date
    payload["_end_date"] = end_date
    return payload


def _default_cities():
    # (city_code, lat, lon)
    return [
        ("BUE", -34.61, -58.38),  # Buenos Aires
        ("SCL", -33.45, -70.66),  # Santiago
        ("MAD", 40.42, -3.70),  # Madrid
        ("MIA", 25.76, -80.19),  # Miami
    ]


def main():
    BASE_DIR = os.getenv("DATA_DIR", "./data")
    run_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=30)).isoformat()
    end_date = (date.today() - timedelta(days=1)).isoformat()
    logger.info("Starting fetch_weather pipeline")
    logger.info(f"Run date: {run_date}, Range: {start_date} → {end_date}")

    data = []
    for code, lat, lon in _default_cities():
        logger.info(f"Fetching city={code} lat={lat} lon={lon}")
        payload = fetch_city_weather(code, lat, lon, start_date, end_date)
        logger.info(f"{code}: Retrieved {len(payload['daily']['time'])} days")
        data.append(payload)

    raw_dir = os.path.join(BASE_DIR, "raw", run_date)
    os.makedirs(raw_dir, exist_ok=True)
    out_path = os.path.join(raw_dir, "weather.json")
    with open(out_path, "w") as f:
        json.dump({"run_date": run_date, "data": data}, f)
    logger.info(f"RAW saved: {out_path}")
    logger.info("fetch_weather pipeline finished OK")


if __name__ == "__main__":
    main()
