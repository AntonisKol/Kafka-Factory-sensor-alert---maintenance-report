"""
File: run_consumer.py

Purpose: Batch consumer that polls Kafka topic
Output: JSON files (human-readable) + Parquet files (for Spark processing)

This is the standalone version for testing.
In production, this function (poll_kafka_to_json) is called by Airflow task_1 every 15 minutes.
"""

from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def poll_kafka_to_json():
    """
    Poll Kafka broker for all available messages in factory-sensors topic.
    Save as JSON and Parquet.
    
    Called by: Airflow task_1 every 15 minutes (in production)
    """
    
    consumer = KafkaConsumer(
        'factory-sensors',
        bootstrap_servers=['localhost:9092'],
        group_id='factory-batch-consumer',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        session_timeout_ms=30000,
        request_timeout_ms=60000
    )
    
    logger.info("Connected to Kafka broker")
    
    readings = []
    timeout_seconds = 30
    
    try:
        logger.info(f"Polling Kafka for {timeout_seconds} seconds...")
        
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < timeout_seconds:
            msg_batch = consumer.poll(timeout_ms=5000, max_records=1000)
            
            if msg_batch:
                for topic_partition, records in msg_batch.items():
                    for record in records:
                        readings.append(record.value)
                        
                logger.info(f"Polled {len(readings)} messages so far...")
            else:
                logger.info("No new messages available")
                break
        
        logger.info(f"Total messages polled: {len(readings)}")
        
        if not readings:
            logger.warning("No readings found in Kafka topic")
            return None
        
        # Create timestamp for file naming
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Save as JSON
        json_output_path = f'/tmp/raw_readings_{timestamp}.json'
        with open(json_output_path, 'w') as f:
            for reading in readings:
                f.write(json.dumps(reading) + '\n')
        
        logger.info(f"Saved JSON to {json_output_path}")
        
        # Convert to DataFrame and save as Parquet
        df = pd.DataFrame(readings)
        
        # Ensure proper data types
        # Accept various ISO formats (with or without timezone). Coerce errors to NaT
        # and then drop invalid timestamps so downstream processing won't fail.
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], utc=True, errors='coerce'
        )
        num_invalid_ts = df['timestamp'].isna().sum()
        if num_invalid_ts:
            logger.warning(f"{num_invalid_ts} invalid timestamps coerced to NaT and will be dropped")
            df = df.dropna(subset=['timestamp'])

        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        parquet_output_path = f'/tmp/raw_readings_{timestamp}.parquet'
        df.to_parquet(parquet_output_path, index=False, compression='snappy')
        
        logger.info(f"Saved Parquet to {parquet_output_path}")
        logger.info(f"Data shape: {df.shape}")
        
        # Mark as processed by committing offsets
        consumer.commit()
        logger.info("Offset committed to Kafka")
        
        return {
            'json_path': json_output_path,
            'parquet_path': parquet_output_path,
            'record_count': len(readings),
            'timestamp': timestamp
        }
    
    except Exception as e:
        logger.error(f"Error during Kafka polling: {e}", exc_info=True)
        raise
    
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


# Standalone execution for testing
if __name__ == '__main__':
    print("\n" + "="*70)
    print("BATCH CONSUMER - Polling Kafka for 30 seconds")
    print("="*70 + "\n")
    
    result = poll_kafka_to_json()
    
    if result:
        print("\n" + "="*70)
        print("CONSUMER COMPLETE")
        print("="*70)
        print(f"JSON:    {result['json_path']}")
        print(f"Parquet: {result['parquet_path']}")
        print(f"Records: {result['record_count']}")
        print("="*70 + "\n")
    else:
        print("\n✗ No readings found!\n")
        exit(1)