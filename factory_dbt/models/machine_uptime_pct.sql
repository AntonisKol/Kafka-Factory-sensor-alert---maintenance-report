{{ config(materialized='table') }}

WITH readings_with_alerts AS (
    SELECT
        r.machine_id,
        COUNT(DISTINCT r.timestamp) as total_readings,
        COUNT(DISTINCT CASE WHEN a.is_anomaly THEN r.timestamp END) as anomaly_readings
    FROM {{ source('raw', 'raw_readings') }} r
    LEFT JOIN {{ source('raw', 'alerts') }} a
        ON r.machine_id = a.machine_id
        AND r.timestamp = a.timestamp
    GROUP BY 1
)
SELECT
    machine_id,
    total_readings,
    anomaly_readings,
    ROUND(100.0 * (total_readings - COALESCE(anomaly_readings, 0)) / total_readings, 2) as uptime_pct
FROM readings_with_alerts
ORDER BY uptime_pct DESC
