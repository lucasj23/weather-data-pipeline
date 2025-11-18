import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from dotenv import load_dotenv
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "./data")
PG_DSN = os.getenv(
    "PG_DSN", "postgresql+psycopg2://postgres:postgres@localhost:5432/weather"
)  # Conexion chain to docker

engine = create_engine(PG_DSN, pool_pre_ping=True)
# Conexion motor to SQL (alchemy). This is the conexion to the DB.
# This allows pandas to use to_sql method to load dataframes directly to the DB (very important).


#  Helper: UPSERT method.
# NOTE: I had to add this because pandas 'to_sql' does not support upserts natively.


def make_upsert_method(conflict_keys: list[str], do_update: bool = False):
    """
    Factory that creates an 'upsert' method for pandas.to_sql() (as I said above, pandas does not support upserts natively).
    - conflict_keys: columns used as unique key for conflict detection.
    - do_update=False → ON CONFLICT DO NOTHING (skip duplicates).
    - do_update=True  → ON CONFLICT DO UPDATE (refresh non-key columns).
    """

    def upsert_method(table, conn, keys, data_iter):
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return
        insert_stmt = postgresql.insert(table.table).values(rows)

        if do_update:
            # Update all non-key columns if conflict occurs
            update_cols = {
                c.name: insert_stmt.excluded[c.name]
                for c in table.table.columns
                if c.name not in conflict_keys
            }
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_keys, set_=update_cols
            )
        else:
            # Do nothing if row already exists (idempotent insert)
            stmt = insert_stmt.on_conflict_do_nothing(index_elements=conflict_keys)

        conn.execute(stmt)

    return upsert_method


# Generic loader function


def load_weather_table(
    parquet_glob: str,
    table_name: str,
    columns: list[str],
    conflict_keys: list[str],
    do_update: bool = False,
) -> int:
    """
    Load multiple Parquet files into Postgres.

    parquet_glob : glob pattern relative to DATA_DIR (e.g. 'gold/*/weather_daily_enriched.parquet'
                   or 'clean/*/weather.parquet')
    table_name   : destination table in Postgres
    columns      : columns to include in insert (extra cols are ignored)
    conflict_keys: columns defining uniqueness constraint
    do_update    : whether to update existing records (default=False)
    """
    logger.info(f"Loading table '{table_name}' into Postgres")
    paths = glob.glob(os.path.join(DATA_DIR, parquet_glob))
    if not paths:
        logger.warning(f"No files found for pattern: {parquet_glob}")
        return 0

    total_rows = 0
    method = make_upsert_method(conflict_keys, do_update)

    for p in paths:
        df = pd.read_parquet(p)
        logger.info(f"Loaded parquet {p} with {len(df)} rows")

        # Derive run_date from folder name .../<run_date>/file.parquet
        run_date = os.path.basename(os.path.dirname(p))
        if "run_date" not in df.columns:
            df["run_date"] = run_date
        else:
            df["run_date"] = df["run_date"].fillna(run_date)

        # Keep only requested columns and drop duplicates on conflict keys
        df = df[[c for c in columns if c in df.columns]].copy()
        if conflict_keys:
            # Filter conflict_keys to only include columns present in the DataFrame
            actual_conflict_keys = [key for key in conflict_keys if key in df.columns]
            df.drop_duplicates(subset=actual_conflict_keys, inplace=True)

        # Insert into Postgres
        df.to_sql(
            table_name,
            engine,
            schema="weather",
            if_exists="append",
            index=False,
            method=method,
        )

        total_rows += len(df)

    logger.info(f"Loaded {total_rows} rows into weather.{table_name}")
    return total_rows


# Loading tables SILVER (clean) + GOLD:

if __name__ == "__main__":
    # 1) SILVER (clean) — from data/clean/<run_date>/weather.parquet
    load_weather_table(
        parquet_glob="clean/*/weather.parquet",
        table_name="weather_silver",
        columns=[
            "run_date",
            "city_code",
            "date",
            "temp_min",
            "temp_max",
            "temp_avg",
            "precip_mm",
        ],
        conflict_keys=["city_code", "date"],  # one row per city & date in silver
        do_update=False,
    )

    # 2) GOLD daily enriched — from data/gold/<run_date>/weather_daily_enriched.parquet
    load_weather_table(
        parquet_glob="gold/*/weather_daily_enriched.parquet",
        table_name="weather_daily",
        columns=[
            "run_date",
            "city_code",
            "date",
            "temp_min",
            "temp_max",
            "temp_avg",
            "temp_range",
            "precip_mm",
        ],
        conflict_keys=["city_code", "date"],
        do_update=False,
    )

    # 3) GOLD monthly KPIs — from data/gold/<run_date>/weather_monthly_kpis.parquet
    load_weather_table(
        parquet_glob="gold/*/weather_monthly_kpis.parquet",
        table_name="weather_monthly_kpis",
        columns=[
            "run_date",
            "city_code",
            "month",
            "avg_temp_min",
            "avg_temp_max",
            "avg_temp_avg",
            "total_precip",
        ],
        conflict_keys=["city_code", "month"],
        do_update=False,
    )

    # 4) GOLD daily KPIs — from data/gold/<run_date>/weather_daily_kpis.parquet
    load_weather_table(
        parquet_glob="gold/*/weather_daily_kpis.parquet",
        table_name="weather_daily_kpis",
        columns=[
            "run_date",
            "city_code",
            "avg_temp_min",
            "avg_temp_max",
            "avg_precip_mm",
        ],
        conflict_keys=["city_code", "run_date"],
        do_update=False,
    )
