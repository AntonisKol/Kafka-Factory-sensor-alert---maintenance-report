from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

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
    
    
    if random.random() < 0.05:
        metric = random.choice(['temperature', 'vibration', 'pressure'])
        if metric == 'temperature':
            base_values[metric] = random.uniform(80, 95)
        elif metric == 'vibration':
            base_values[metric] = random.uniform(6, 8)
        else:
            base_values[metric] = random.uniform(35, 50)
    
    return base_values

while True:
    for machine_id in MACHINES:
        reading = generate_reading()
        for metric_name, value in reading.items():
            msg = {
                'timestamp': datetime.now(time.timezone.utc).isoformat()
,
                'machine_id': machine_id,
                'metric_name': metric_name,
                'value': value
            }
            producer.send('factory-sensors', msg)
    time.sleep(1)