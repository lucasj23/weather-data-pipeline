# Weather Data Pipeline

## Overview

This project implements a modern, end-to-end data pipeline fully orchestrated with **Apache Airflow** and deployed using **Docker**, designed to process both historical and real-time weather data.

The goal is to build an **automated, reproducible, and modular** ETL workflow that:

- Extracts weather data from a public API (raw layer)  
- Cleans, normalizes and validates it (silver layer)  
- Computes derived metrics and KPIs (gold layer)  
- Loads results into a PostgreSQL Data Warehouse (Dockerized)

The project follows best practices in Data Engineering: ELT, orchestration, containerized infrastructure, unit testing, and CI/CD.

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
│   └── test_gold_weather.py
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
User: `****`  
Password: `****`

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


