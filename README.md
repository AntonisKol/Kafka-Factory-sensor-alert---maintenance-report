# Factory sensor alerts and maintenance reports

Simulated factory sensors stream through Kafka. Every 15 minutes Airflow flags anomalies and loads them into Snowflake. A daily job builds dbt models and emails a maintenance report.

## What runs

- `producer.py` streams temperature, vibration, and pressure for 10 machines to the Kafka topic `factory-sensors` until you stop it.
- `factory_pipeline.py` is the Airflow DAG `factory_maintenance_pipeline`. Every 15 minutes it polls Kafka (`consumer.py`), flags anomalies (`spark_anomaly_detection.py`), and loads Snowflake (`snowflake_loader.py`).
- `factory_pipeline_eod.py` is the Airflow DAG `factory_maintenance_eod`. Daily at 23:00 it runs `dbt run` in `factory_dbt/`, then `send_report.py`.

`DIAGRAM.md` shows the same flow.

## Configure

Copy the example env file and fill it in. `.env` is gitignored.

```bash
cp .env.example .env
```

`snowflake_loader.py` reads `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD` from `.env`. It writes database `KAFKA_DEMO`, schema `PUBLIC`, using warehouse `DEV_LOADING_WH`.

`send_report.py` reads the full Snowflake block from `.env` (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`) plus `EMAIL_SENDER`, `EMAIL_PASSWORD`, and `EMAIL_RECIPIENT`. Mail goes out through `smtp.gmail.com`.

Set `FACTORY_PROJECT_DIR` only when the DAG files are copied into an Airflow dags folder and the rest of this project lives somewhere else. The Spark task and the end-of-day dbt task `cd` into that path. The Python tasks import `consumer` and `snowflake_loader` from the DAG file's folder.

## Services it expects

- Kafka at `localhost:9092`
- Batch files under `/tmp`: `raw_readings_*.json` and `flagged_readings_*.parquet`
- Snowflake tables `raw_readings` and `alerts`, as in `factory_dbt/models/sources.yml`
- A dbt profile named `factory_dbt` in your local `~/.dbt/profiles.yml` (that file is not in this repo)

The email report queries `DBT_MODELS.alert_frequency` and `DBT_MODELS.machine_uptime_pct`.

## Anomaly rules

A reading is flagged when its own value is outside these bounds:

- temperature: 20–80
- vibration: 0–5
- pressure: 10–30

`producer.py` emits normal readings inside those bounds (temperature 35–75, vibration 1–4, pressure 12–28) and injects a spike on about 5% of readings. `rolling_avg_5min` is stored as context. It is not what gets flagged.

## dbt models

In `factory_dbt/models/`:

- `hourly_sensor_aggregates`
- `machine_uptime_pct`
- `alert_frequency`

## Python packages

`kafka-python`, `pandas`, `pyspark`, `snowflake-connector-python`, `python-dotenv`, Apache Airflow, and dbt with the Snowflake adapter.
