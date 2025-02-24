import json
import time
from kafka import KafkaProducer

import sys
import os

# Add the root project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.generate_traffic import generate_traffic_data  # Import traffic data generator

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "traffic"

if __name__ == "__main__":
    while True:
        traffic_event = generate_traffic_data()
        producer.send(TOPIC_NAME, value=traffic_event)
        print(f"📤 Sent to Kafka [{TOPIC_NAME}]: {json.dumps(traffic_event, indent=4)}")
        time.sleep(5)  # Generate new data every 5 seconds
