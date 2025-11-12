CREATE SCHEMA IF NOT EXISTS weather;

-- kpis daily - Datos diarios por ciudad y fecha

CREATE TABLE IF NOT EXISTS weather.weather_daily (
  run_date   date NOT NULL,
  city_code  text NOT NULL,
  date       date NOT NULL,
  temp_min   double precision,
  temp_max   double precision,
  temp_avg   double precision,
  temp_range double precision,
  precip_mm  double precision,
  PRIMARY KEY (city_code, date)
);

-- Tabla weather.weather_daily_kpis (KPIs diarios)
CREATE TABLE IF NOT EXISTS weather.weather_daily_kpis (
    run_date DATE NOT NULL,
    city_code TEXT NOT NULL,
    avg_temp_min DOUBLE PRECISION,
    avg_temp_max DOUBLE PRECISION,
    avg_precip_mm DOUBLE PRECISION,
    PRIMARY KEY (city_code, run_date)
);

-- KPIs mensuales agregados por ciudad y mes
CREATE TABLE IF NOT EXISTS weather.weather_monthly_kpis (
  run_date     date NOT NULL,
  city_code    text NOT NULL,
  month        text NOT NULL,
  avg_temp_min double precision,
  avg_temp_max double precision,
  avg_temp_avg double precision,
  total_precip double precision,
  PRIMARY KEY (city_code, month)
);

-- Creamos Silver table
CREATE TABLE IF NOT EXISTS weather.weather_silver (
    city_code TEXT NOT NULL,
    date DATE NOT NULL,
    temp_min DOUBLE PRECISION,
    temp_max DOUBLE PRECISION,
    temp_avg DOUBLE PRECISION,
    temp_range DOUBLE PRECISION,
    precip_mm DOUBLE PRECISION,
    run_date DATE,
    PRIMARY KEY (city_code, date)
);
