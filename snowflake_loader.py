"""
File: snowflake_loader.py

Task: Load raw JSON and flagged Parquet to Snowflake
- raw_readings table: raw JSON from Kafka consumer
- alerts table: flagged anomalies from PySpark
"""

import snowflake.connector
import json
import pandas as pd
import glob
from datetime import datetime
import os
# Snowflake credentials
SNOWFLAKE_CONFIG = {
    'account': 'GCWFKTK-JR15161',
    'user': 'ANTONIS',
    'password': os.environ.get('SNOWFLAKE_PASSWORD'),  # Read from env var
    'warehouse': 'DEV_LOADING_WH',
    'database': 'KAFKA_DEMO',
    'schema': 'ACCOUNTADMIN'
}

def connect_snowflake():
    """Connect to Snowflake"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("✓ Connected to Snowflake")
        return conn
    except Exception as e:
        print(f"✗ Snowflake connection failed: {e}")
        raise

def load_raw_readings(conn):
    """Load raw JSON readings to raw_readings table"""
    json_files = glob.glob('/tmp/raw_readings_*.json')
    if not json_files:
        print("⚠ No raw_readings JSON files found")
        return 0
    
    latest_file = max(json_files)
    print(f"Loading raw readings from: {latest_file}")
    
    with open(latest_file, 'r') as f:
        lines = f.readlines()
    
    cursor = conn.cursor()
    count = 0
    
    for line in lines:
        try:
            record = json.loads(line)
            cursor.execute(
                "INSERT INTO raw_readings (timestamp, machine_id, metric_name, value) "
                "VALUES (%s, %s, %s, %s)",
                (record['timestamp'], record['machine_id'], 
                 record['metric_name'], record['value'])
            )
            count += 1
        except Exception as e:
            print(f"  Error loading record: {e}")
    
    conn.commit()
    print(f"✓ Loaded {count} raw readings to Snowflake")
    return count

def load_alerts(conn):
    """Load flagged anomalies from Parquet to alerts table"""
    parquet_files = glob.glob('/tmp/flagged_readings_*.parquet')
    if not parquet_files:
        print("⚠ No flagged_readings Parquet files found")
        return 0
    
    latest_file = max(parquet_files)
    print(f"Loading alerts from: {latest_file}")
    
    df = pd.read_parquet(latest_file)
    
    # Convert timestamp to string
    df['timestamp'] = df['timestamp'].astype(str)
    
    cursor = conn.cursor()
    count = 0
    
    for _, row in df.iterrows():
        try:
            cursor.execute(
                "INSERT INTO alerts (timestamp, machine_id, metric_name, value, rolling_avg_5min, is_anomaly) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (row['timestamp'], row['machine_id'], row['metric_name'],
                 float(row['value']), float(row['rolling_avg_5min']), bool(row['is_anomaly']))
            )
            count += 1
        except Exception as e:
            print(f"  Error loading alert: {e}")
    
    conn.commit()
    print(f"✓ Loaded {count} alerts to Snowflake")
    return count

def main():
    """Main: Load both raw and flagged data"""
    try:
        conn = connect_snowflake()
        
        raw_count = load_raw_readings(conn)
        alert_count = load_alerts(conn)
        
        print(f"\n✓ Loader complete: {raw_count} readings, {alert_count} alerts")
        
        conn.close()
        
    except Exception as e:
        print(f"✗ Loader failed: {e}")
        raise

if __name__ == '__main__':
    main()
