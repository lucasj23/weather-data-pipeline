from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Using BashOperator instead of PythonOperator because each ETL step (fetch, clean, gold, load) is already implemented as an independent Python script.
# This approach keeps the DAG simple and avoids managing imports, paths, and environment variables inside Airflow.

default_args = {"retries": 3, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 8 * * *",  # daily - 08:00AM
    catchup=False,
    default_args=default_args,
    tags=["weather"],
) as dag:

    fetch = BashOperator(
        task_id="fetch",
        bash_command="cd /opt/pipeline && python ingestion/fetch_weather.py",
    )
    clean = BashOperator(
        task_id="clean",
        bash_command="cd /opt/pipeline && python transformations/clean_weather.py",
    )
    gold = BashOperator(
        task_id="gold", bash_command="cd /opt/pipeline && python models/gold_weather.py"
    )
    load = BashOperator(
        task_id="load", bash_command="cd /opt/pipeline && python loaders/load_to_pg.py"
    )

    fetch >> clean >> gold >> load
