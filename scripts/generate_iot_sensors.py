import json
import random
import time
from datetime import datetime, timedelta
import os

# Load external data from text files
def load_data(filename):
    # Get the absolute path of the script
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
    # Go up one level (from 'scripts' to project root) and into 'data'
    DATA_PATH = os.path.join(BASE_DIR, "..", "data", filename)  

    # Open and read the file
    with open(DATA_PATH, "r", encoding="utf-8") as file:
        return file.readlines()

reported_by_sources = load_data("reported_by.txt")

def generate_iot_sensor_data():
    return {
        "sensor_id": f"IOT-{random.randint(10000, 99999)}",
        "timestamp": (datetime.utcnow() + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "building": {
            "name": random.choice(["Tech Park Tower", "Downtown Office", "University Library"]),
            "building_id": f"BID-{random.randint(100, 999)}",
            "floor": random.randint(1, 25),
            "room_number": f"{random.randint(100, 999)}"
        },
        "occupancy": {
            "people_count": random.randint(0, 30),
            "capacity_limit": random.randint(20, 50),
            "motion_detected": random.choice([True, False]),
            "entry_exit_logs": [
                {
                    "timestamp": (datetime.utcnow() - timedelta(minutes=random.randint(1, 60))).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "person_id": f"UID-{random.randint(1000, 9999)}",
                    "entry": random.choice([True, False])
                } for _ in range(random.randint(1, 5))
            ]
        },
        "HVAC_status": {
            "temperature_setpoint_celsius": random.randint(18, 25),
            "current_temperature_celsius": round(random.uniform(18, 30), 2),
            "humidity_percent": random.randint(30, 70),
            "system_active": random.choice([True, False]),
            "air_filter_quality_percent": random.randint(50, 100),
            "co2_levels_ppm": random.randint(300, 800),
            "maintenance_alerts": [
                {
                    "alert_type": random.choice(["Filter Replacement", "Temperature Deviation", "Humidity Anomaly"]),
                    "severity": random.choice(["Low", "Medium", "High"]),
                    "alert_timestamp": (datetime.utcnow() - timedelta(minutes=random.randint(1, 60))).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            ] if random.random() < 0.2 else []
        },
        "energy_efficiency": {
            "power_usage_watts": random.randint(500, 5000),
            "smart_lighting_active": random.choice([True, False]),
            "energy_savings_mode": random.choice([True, False]),
            "consumption_trends": [
                {
                    "hour": f"{hour}:00",
                    "power_usage": random.randint(200, 2000)
                } for hour in range(0, 24, 2)
            ]
        },
        "reported_by": random.choice(reported_by_sources)
    }

if __name__ == "__main__":
    while True:
        data = generate_iot_sensor_data()
        print(json.dumps(data, indent=4))
        time.sleep(5)  # Generate data every 30 minutes
