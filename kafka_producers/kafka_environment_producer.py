import json
import time
from kafka import KafkaProducer
import sys
import os

# Add the root project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.generate_environment import generate_environment_data


# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "environment"

if __name__ == "__main__":
    while True:
        environment_event = generate_environment_data()
        producer.send(TOPIC_NAME, value=environment_event)
        print(f"📤 Sent to Kafka [{TOPIC_NAME}]: {json.dumps(environment_event, indent=4)}")
        time.sleep(5) 
