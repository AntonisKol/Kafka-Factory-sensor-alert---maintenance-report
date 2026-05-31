"""

Purpose: Batch consumer that polls Kafka topic every 15 minutes (triggered by Airflow)
Output: JSON files (human-readable) for Spark processing

This is called by Airflow task_1 in factory_pipeline.py
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
    Save as JSON.
    
    Called by: Airflow task_1 every 15 minutes
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
        
        consumer.commit()
        logger.info("Offset committed to Kafka")
        
        return {
            'json_path': json_output_path,
            'record_count': len(readings),
            'timestamp': timestamp
        }
    
    except Exception as e:
        logger.error(f"Error during Kafka polling: {e}", exc_info=True)
        raise
    
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")