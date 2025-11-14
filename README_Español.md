# Weather Data Pipeline

### Descripción general

Este proyecto implementa un pipeline de datos moderno, completamente orquestado con Apache Airflow y desplegado mediante Docker, para procesar información meteorológica histórica y reciente.

El objetivo del pipeline es construir un flujo automatizado, reproducible y modular que:
```
	- Extrae datos meteorológicos desde una API pública (capa raw).
	- Limpia, normaliza y valida los datos (silver).
	- Genera métricas derivadas y KPIs (gold).
	- Carga los resultados en un Data Warehouse PostgreSQL dentro de Docker.
```
El proyecto está diseñado siguiendo buenas prácticas de Data Engineering, ELT, versionado con GitHub, testing automático y CI/CD.

### Arquitectura del Pipeline

```
         API (raw)
             ↓
   Clean / Transform (silver)
             ↓
 Feature Engineering + KPIs (gold)
             ↓
   PostgreSQL (Data Warehouse)
             ↓
 Orquestación: Apache Airflow
```

DAG Principal: 
 ```fetch → clean → gold → load ```.
 
Cada etapa corre como un script independiente dentro del contenedor de Airflow.


### Estructura del Proyecto
 ```
weather-data-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── weather_pipeline.py
│   ├── db_data/        (ignorado - metadata interna de Airflow)
│   └── logs/           (ignorado)
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
├── data/               (ignorado - datasets generados)
├── docker-compose.yml
├── init.sql
├── .gitignore
└── README.md
 ```

### Cómo levantar el proyecto
  Desde la carpeta raíz:
 ```docker compose up -d ```

Airflow estará disponible en:
http://localhost:8080
Usuario: ****
Contraseña: ****

El DAG weather_pipeline puede ejecutarse manualmente o programarse para correr automáticamente.

### Almacenamiento (PostgreSQL Warehouse)

El Data Warehouse corre en un contenedor propio dentro de Docker Compose.
Conexión (valores por defecto):
Host: localhost
Port: 5439
Database: weatherdb
User: ****
Password: ****

Tablas generadas automáticamente:
	•	weather_silver
	•	weather_daily
	•	weather_daily_kpis
	•	weather_monthly_kpis


### KPIS Calculados

KPIs diarios
	•	Temperatura mínima promedio
	•	Temperatura máxima promedio
	•	Precipitación promedio

KPIs mensuales
	•	Promedio de temperatura mínima
	•	Promedio de temperatura máxima
	•	Promedio de temperatura media
	•	Precipitación total del mes

Los KPIs se calculan a partir de los datos limpios y se guardan como archivos Parquet antes de ser cargados en la base de datos.

### Testing + CI/CD
El repositorio incluye tests unitarios en Python (pytest) ubicados en:
tests/

Están integrados con GitHub Actions, lo que permite ejecutar los tests automáticamente en cada push o Pull Request.

Workflow ubicado en:
 ``` .github/workflows/tests.yml ```

Esto garantiza reproducibilidad y calidad del ETL.


### Tecnologías utilizadas
	•	Python 3.12
	•	Docker + Docker Compose
	•	Airflow 3
	•	PostgreSQL
	•	Pandas
	•	SQLAlchemy
	•	Pytest
	•	GitHub Actions



