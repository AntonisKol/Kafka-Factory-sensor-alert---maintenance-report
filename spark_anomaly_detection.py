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
# Ordered by epoch seconds so the range covers the last 5 minutes of readings
window_spec = Window.partitionBy('machine_id', 'metric_name') \
    .orderBy(col('timestamp').cast('long')) \
    .rangeBetween(-300, 0)

df_with_rolling = df.withColumn(
    'rolling_avg_5min',
    avg('value').over(window_spec)
)

# Flag anomalies on each reading's own value. A single spike barely moves
# the rolling average, so checking the average would almost never trip.
# rolling_avg_5min is kept as context for the alert.
df_flagged = df_with_rolling.withColumn(
    'is_anomaly',
    when(col('metric_name') == 'temperature',
         (col('value') > THRESHOLDS['temperature']['max']) |
         (col('value') < THRESHOLDS['temperature']['min'])
    ).when(col('metric_name') == 'vibration',
         (col('value') > THRESHOLDS['vibration']['max']) |
         (col('value') < THRESHOLDS['vibration']['min'])
    ).when(col('metric_name') == 'pressure',
         (col('value') > THRESHOLDS['pressure']['max']) |
         (col('value') < THRESHOLDS['pressure']['min'])
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