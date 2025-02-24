import json
import time
from kafka import KafkaProducer

import sys
import os

# Add the root project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.generate_iot_sensors import generate_iot_sensor_data  # Import IoT sensor data generator

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "iot_sensors"

if __name__ == "__main__":
    while True:
        iot_sensor_event = generate_iot_sensor_data()
        producer.send(TOPIC_NAME, value=iot_sensor_event)
        print(f"📤 Sent to Kafka [{TOPIC_NAME}]: {json.dumps(iot_sensor_event, indent=4)}")
        time.sleep(5)  # Generate new data every 5 seconds
