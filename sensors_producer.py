"""
File: run_producer.py
Simplified sensor producer - emits for 20 seconds
"""

from kafka import KafkaProducer
import json
import time
import random
import sys
from datetime import datetime, timezone

print("\n" + "="*70)
print("SENSOR PRODUCER - 20 seconds")
print("="*70 + "\n")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

MACHINES = [f'M{i:03d}' for i in range(1, 11)]

THRESHOLDS = {
    'temperature': {'min': 20, 'max': 80, 'unit': 'C'},
    'vibration': {'min': 0, 'max': 5, 'unit': 'Hz'},
    'pressure': {'min': 10, 'max': 30, 'unit': 'bar'}
}

def generate_reading():
    base_values = {
        'temperature': random.uniform(35, 75),
        'vibration': random.uniform(1, 4),
        'pressure': random.uniform(12, 28)
    }
    
    # 5% chance of spike
    if random.random() < 0.05:
        metric = random.choice(['temperature', 'vibration', 'pressure'])
        if metric == 'temperature':
            base_values[metric] = random.uniform(80, 95)
        elif metric == 'vibration':
            base_values[metric] = random.uniform(6, 8)
        else:
            base_values[metric] = random.uniform(35, 50)
    
    return base_values

start_time = time.time()
message_count = 0

print("Starting to emit sensor readings...")
print("(10 machines × 3 metrics = 30 messages per second)\n")

while time.time() - start_time < 20:
    for machine_id in MACHINES:
        reading = generate_reading()
        for metric_name, value in reading.items():
            msg = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'machine_id': machine_id,
                'metric_name': metric_name,
                'value': value
            }
            producer.send('factory-sensors', msg)
            message_count += 1
    
    elapsed = time.time() - start_time
    remaining = 20 - elapsed
    print(f"\r[{elapsed:5.1f}s] Sent {message_count:4d} messages (ETA: {remaining:5.1f}s)", end='', flush=True)
    time.sleep(1)

producer.flush()
producer.close()

elapsed_total = time.time() - start_time
print(f"\n\n✓ Producer complete!")
print(f"  Total time: {elapsed_total:.1f}s")
print(f"  Total messages: {message_count}")
print(f"  Rate: {message_count/elapsed_total:.1f} msgs/sec")
print(f"  ~600 messages now in Kafka topic\n")