# Factory Sensor Alert & Maintenance Report Pipeline

## Architecture Diagram

```
┌─────────────────┐
│  producer.py    │  Every 1 sec → 30 msgs/sec
│  (10 machines)  │
└────────┬────────┘
         │ JSON
         ▼
┌─────────────────────────────┐
│   Kafka Topic               │
│   factory-sensors (buffer)  │
└────────┬────────────────────┘
         │
    ┌────┴──────────────────────────────┐
    │                                   │
    ▼                                   ▼
┌──────────────┐              ┌──────────────────┐
│   Airflow    │              │  consumer.py     │
│ Every 15 min │──────────→   │  Poll 30 sec     │
└──────────────┘              └────────┬─────────┘
                                       │ JSON files
                                       ▼
                              /tmp/raw_readings_*.json
                                       │
                                       ▼
                         ┌──────────────────────────┐
                         │ spark_anomaly_detection  │
                         │ Rolling avg + threshold  │
                         │ flags is_anomaly = T/F   │
                         └────────┬─────────────────┘
                                  │ Parquet
                                  ▼
                         /tmp/flagged_readings_*.parquet
                                  │
                         ┌────────┴──────────────┐
                         │                       │
                         ▼                       ▼
                    (raw JSON) ◄────────────────┐
                         │                      │
                         └──────────┬───────────┘
                                    ▼
                    ┌───────────────────────────────────┐
                    │   snowflake_loader.py             │
                    │   Loads raw_readings + alerts     │
                    └────────────┬──────────────────────┘
                                 ▼
                    ┌───────────────────────────────────┐
                    │   Snowflake                       │
                    │   KAFKA_DEMO.PUBLIC               │
                    │   • raw_readings (all data)       │
                    │   • alerts (flagged anomalies)    │
                    └────────────┬──────────────────────┘
                                 │
                         ┌───────┴─────────────────┐
                         │                         │
                         ▼                         ▼
                    ┌──────────────┐        ┌─────────────────┐
                    │   Airflow    │        │   dbt models    │
                    │ Daily 23:00  │───────→│ • hourly_agg    │
                    └──────────────┘        │ • uptime_pct    │
                                           │ • alert_freq    │
                                           └────────┬────────┘
                                                    │
                                                    ▼
                                           ┌─────────────────┐
                                           │ send_report.py  │
                                           │ Query dbt       │
                                           │ Send email      │
                                           └─────────────────┘
```
