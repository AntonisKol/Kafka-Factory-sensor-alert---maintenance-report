{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    machine_id,
    metric_name,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(*) as reading_count
FROM {{ source('raw', 'raw_readings') }}
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3

-- Round each timestamp to the hour
-- Group readings by hour + machine + metric
-- Calculate avg, min, max, count for each group
-- Return newest hours first