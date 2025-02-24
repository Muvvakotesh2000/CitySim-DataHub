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

# Load external data
building_names = load_data("building_names.txt")

def generate_utilities_data():
    return {
        "event_id": f"UTIL-{random.randint(10000, 99999)}",
        "timestamp": (datetime.utcnow() + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "building": {
            "name": random.choice(building_names),
            "building_id": f"BID-{random.randint(100, 999)}",
            "type": random.choice(["Government", "Commercial", "Residential"]),
            "location": {
                "latitude": round(random.uniform(34.05, 34.15), 6),
                "longitude": round(random.uniform(-118.25, -118.20), 6)
            }
        },
        "energy_consumption": {
            "electricity_kwh": random.randint(500, 5000),
            "gas_m3": random.randint(100, 1000),
            "renewable_energy_percent": round(random.uniform(10, 80), 2),
            "power_outage_reported": random.choice([True, False])
        },
        "water_usage": {
            "liters_used": random.randint(20000, 80000),
            "leak_detected": random.choice([True, False]),
            "contamination_detected": random.choice([True, False])
        },
        "waste_collection": {
            "bins_collected": random.randint(5, 50),
            "bin_fill_level_percent": random.randint(20, 100),
            "recycling_percentage": random.randint(10, 80),
            "hazardous_waste_detected": random.choice([True, False])
        }
    }

if __name__ == "__main__":
    while True:
        data = generate_utilities_data()
        print(json.dumps(data, indent=4))
        time.sleep(5)  # Generates data every 30 minutes
