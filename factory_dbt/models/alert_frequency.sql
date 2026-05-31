{{ config(materialized='table') }}

SELECT
    machine_id,
    metric_name,
    COUNT(*) as alert_count,
    MAX(timestamp) as last_alert
FROM {{ source('raw', 'alerts') }}
WHERE is_anomaly = TRUE
GROUP BY 1, 2
ORDER BY alert_count DESC, 1, 2

-- Count anomalies per machine and metric
-- Group alerts by machine_id and metric_name  
-- Calculate total anomalies and latest alert timestamp
-- Sort by most anomalies first
