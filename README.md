# Weather Data Pipeline

## Overview

This project implements a modern, end-to-end data pipeline fully orchestrated with **Apache Airflow** and deployed using **Docker**, designed to process both historical and real-time weather data.

The goal is to build an **automated, reproducible, and modular** ETL workflow that:

- Extracts weather data from a public API (raw layer)  
- Cleans, normalizes and validates it (silver layer)  
- Computes derived metrics and KPIs (gold layer)  
- Loads results into a PostgreSQL Data Warehouse (Dockerized)

The project follows best practices in Data Engineering: ELT, orchestration, containerized infrastructure, unit testing, and CI/CD.
Also, the project can serve as the foundation for analytical use cases such as building dashboards, querying historical climate behavior, identifying long-term temperature trends, and supporting future forecasting or BI applications. The processed data stored in PostgreSQL enables flexible querying and further visualization in tools like Tableau, Power BI, or Python notebooks.
---

## Pipeline Architecture

```
         API (raw)
             ↓
   Clean / Transform (silver)
             ↓
 Feature Engineering + KPIs (gold)
             ↓
   PostgreSQL (Data Warehouse)
             ↓
  Orchestration: Apache Airflow
```

Main DAG:  
`fetch → clean → gold → load`

---

## Project Structure

```
weather-data-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── weather_pipeline.py
│   ├── db_data/        (ignored)
│   └── logs/           (ignored)
│
├── ingestion/
│   └── fetch_weather.py
│
├── transformations/
│   └── clean_weather.py
│
├── models/
│   └── gold_weather.py
│
├── loaders/
│   └── load_to_pg.py
│
├── tests/
│   ├── test_clean_weather.py
│   └── test_fetch_weather.py
│
├── data/               (ignored)
├── docker-compose.yml
├── init.sql
├── .gitignore
└── README.md
```

---

## How to Run

From the project root:

```
docker compose up -d
```

Airflow UI → http://localhost:8080  
User: airflow  
Password: airflow

Trigger the `weather_pipeline` DAG manually or schedule it.

---

## PostgreSQL Warehouse

Default connection (dummy values):

```
Host: localhost
Port: 5433
Database: ****
User: ****
Password: ****
```

**Note:** Inside the Docker container, PostgreSQL listens on its default internal port `5432`.  
To avoid conflicts with any local PostgreSQL installation on the host machine, the service is exposed on port `5433` externally using the mapping `5433:5432` in `docker-compose.yml`.

Tables generated:

- weather_silver  
- weather_daily  
- weather_daily_kpis  
- weather_monthly_kpis  

---

## KPIs Generated

### Daily KPIs
- Avg minimum temperature  
- Avg maximum temperature  
- Avg precipitation  

### Monthly KPIs
- Avg minimum temperature  
- Avg maximum temperature  
- Avg temperature  
- Total precipitation  

---

## Testing & CI/CD

Tests are located in:

```
tests/
```

GitHub Actions workflow automatically runs:

- Dependency installation  
- Linting  
- Unit tests  

Workflow:

```
.github/workflows/tests.yml
```

---

## Technologies Used

- Python 3.12  
- Docker & Docker Compose  
- Apache Airflow  
- PostgreSQL  
- Pandas  
- SQLAlchemy  
- Pytest  
- GitHub Actions  



# Weather Data Pipeline

## Overview

This project implements a fully automated, end-to-end weather data pipeline orchestrated with **Apache Airflow** and deployed using **Docker**. It processes **historical and daily real‑time weather data** for the following cities:

- Buenos Aires (BUE)
- Santiago de Chile (SCL)
- Madrid (MAD)
- Miami (MIA)

The pipeline is designed to be **modular, reproducible, and production‑ready**, following modern Data Engineering best practices.  
It supports historical backfilling through the environment variable **`DAYS_BACK`**, allowing control over how many past days to ingest.

The system automatically:

- Extracts raw weather data from the **Open‑Meteo API** (no API key required)
- Cleans and normalizes the dataset into a Silver layer  
- Computes enriched features and KPIs in the Gold layer  
- Loads all results into a Dockerized PostgreSQL Warehouse  
- Executes daily via an Airflow‑scheduled DAG  

---

## Pipeline Architecture

```
         API (raw)
             ↓
   Clean / Transform (silver)
             ↓
 Feature Engineering + KPIs (gold)
             ↓
   PostgreSQL (Data Warehouse)
             ↓
  Orchestration: Apache Airflow
```

Main DAG:  
`fetch → clean → gold → load`

---

## Airflow DAG

The orchestrator of the entire workflow is the DAG **`weather_pipeline`**, located in:

```
airflow/dags/weather_pipeline.py
```

The DAG runs every day at **08:00 AM** and executes each ETL step as an independent Python script:

- `ingestion/fetch_weather.py`
- `transformations/clean_weather.py`
- `models/gold_weather.py`
- `loaders/load_to_pg.py`

This design keeps the pipeline modular and easy to debug.

---

## Project Structure

```
weather-data-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── weather_pipeline.py
│   ├── db_data/        (ignored)
│   └── logs/           (ignored)
│
├── ingestion/
│   └── fetch_weather.py
│
├── transformations/
│   └── clean_weather.py
│
├── models/
│   └── gold_weather.py
│
├── loaders/
│   └── load_to_pg.py
│
├── sql/
│   └── init.sql
│
├── tests/
│   ├── test_clean_weather.py
│   └── test_fetch_weather.py
│
├── data/               (ignored)
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

## Folder Breakdown

### `ingestion/`
Fetches weather data from Open‑Meteo and writes raw parquet files.

### `transformations/`
Cleans the raw data, fixes types, normalizes formats, validates schema, and writes Silver data.

### `models/`
Builds the Gold layer, including enriched features and all **daily** and **monthly** KPIs.

### `loaders/`
Loads Silver/Gold tables into PostgreSQL using SQLAlchemy.

### `airflow/`
Holds the DAG, log directories, and metadata used by Airflow.

### `sql/`
Contains the initialization script for automatically creating database tables inside the PostgreSQL container.

### `tests/`
Includes unit tests validating Silver and Gold transformations.

---

## How to Run the Pipeline

### 1. Start the full stack

From the project root:

```
docker compose up --build -d
```

### 2. Access Airflow

```
http://localhost:8080
User: airflow
Password: airflow
```

### 3. Trigger the DAG

- Open Airflow UI
- Search for **weather_pipeline**
- Click **Trigger DAG**

Silver and Gold outputs are written under:

```
data/silver/<run_date>/
data/gold/<run_date>/
```

## Environment Variables (`.env`)

This project requires a `.env` file in the project root directory.  
Docker Compose and Airflow use these environment variables to correctly initialize the services.

Create the file by copying the example:

```bash
cp .env.example .env
```

Then replace the placeholder values with your own:

```env
AIRFLOW_UID=50000
PG_DSN=postgresql+psycopg2://<USER>:<PASSWORD>@postgres-pipeline:5432/weather
```

## Managing Docker Services

To stop, pause, restart, or rebuild the pipeline infrastructure, use the following Docker Compose commands from the project root:

### **Stop all services**
```bash
docker compose down
```

### **Pause running services**
```bash
docker compose pause
```

### **Resume paused services**
```bash
docker compose unpause
```

### **Restart services**
```bash
docker compose restart
```

### **Rebuild the images and start everything**
```bash
docker compose up --build -d
```

These commands allow you to fully control the Airflow and PostgreSQL containers used by the pipeline.

---

## PostgreSQL Warehouse

The project includes a dedicated PostgreSQL instance running inside Docker.  
It initializes itself automatically using `sql/init.sql`.

Default connection (values masked intentionally):

```
Host: localhost
Port: 5433
Database: ****
User: ****
Password: ****
```

Tables created:

- `weather_silver`
- `weather_daily`
- `weather_daily_kpis`
- `weather_monthly_kpis`

---

## KPIs Generated

### Daily KPIs
- Average minimum temperature  
- Average maximum temperature  
- Average precipitation  

### Monthly KPIs
- Average minimum temperature  
- Average maximum temperature  
- Average temperature  
- Total precipitation  

---

## Logging

All major steps include structured logging using Python’s `logging` module.  
Airflow captures these logs automatically, enabling visibility into:

- Step execution status  
- Row counts  
- Missing/invalid data  
- Failures with stack traces  

---

## Testing & CI/CD

Unit tests are available under:

```
tests/
```

GitHub Actions runs automatically on each push:

- Dependency installation  
- Linting (flake8)  
- Unit tests (pytest)

Workflow file:

```
.github/workflows/tests.yml
```

This ensures consistent code quality and pipeline correctness.

---

## Technologies Used

- Python 3.12  
- Docker & Docker Compose  
- Apache Airflow  
- PostgreSQL  
- Pandas  
- SQLAlchemy  
- Pytest  
- GitHub Actions