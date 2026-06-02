from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, when
import glob
from datetime import datetime

spark = SparkSession.builder \
    .appName("FactoryAnomalyDetection") \
    .getOrCreate()

THRESHOLDS = {
    'temperature': {'min': 20, 'max': 80},
    'vibration': {'min': 0, 'max': 5},
    'pressure': {'min': 10, 'max': 30}
}

json_files = glob.glob('/tmp/raw_readings_*.json')
if not json_files:
    raise Exception("No JSON files found")

latest_file = max(json_files)
df = spark.read.json(latest_file)

print(f"Read {df.count()} records from {latest_file}")

# Converted timestamp string to timestamp type because Data from Kafka arrives as strings
df = df.withColumn('timestamp', col('timestamp').cast('timestamp'))

# Defined window that separates data into independent groups
window_spec = Window.partitionBy('machine_id', 'metric_name') \
    .orderBy(col('timestamp')) \
    .rowsBetween(-100, 0)  # Last 100 rows (approximately 5 min)

df_with_rolling = df.withColumn(
    'rolling_avg_5min',
    avg('value').over(window_spec)
)

# Flag anomalies
df_flagged = df_with_rolling.withColumn(
    'is_anomaly',
    when(col('metric_name') == 'temperature',
         (col('rolling_avg_5min') > THRESHOLDS['temperature']['max']) |
         (col('rolling_avg_5min') < THRESHOLDS['temperature']['min'])
    ).when(col('metric_name') == 'vibration',
         (col('rolling_avg_5min') > THRESHOLDS['vibration']['max']) |
         (col('rolling_avg_5min') < THRESHOLDS['vibration']['min'])
    ).when(col('metric_name') == 'pressure',
         (col('rolling_avg_5min') > THRESHOLDS['pressure']['max']) |
         (col('rolling_avg_5min') < THRESHOLDS['pressure']['min'])
    ).otherwise(False)
)

# Filter to flagged records only
alerts_df = df_flagged.filter(col('is_anomaly') == True)

# Save flagged records
timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
alerts_output_path = f'/tmp/flagged_readings_{timestamp}.parquet'
alerts_df.write.parquet(alerts_output_path, mode='overwrite')

print(f"✓ Flagged {alerts_df.count()} anomalies")
print(f"  Output: {alerts_output_path}")

spark.stop()