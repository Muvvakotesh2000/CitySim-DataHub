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

incident_types = load_data("public_services_incident_types.txt")
reported_by_sources = load_data("reported_by.txt")

# Generate realistic emergency response data
def generate_public_services_data():
    return {
        "incident_id": f"EMS-{random.randint(10000, 99999)}",
        "timestamp": (datetime.utcnow() + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "incident_type": random.choice(incident_types),
        "reported_by": random.choice(reported_by_sources),
        "location": {
            "building": random.choice(["Residential Complex A", "Downtown Mall", "Central Park", "University Campus"]),
            "latitude": round(random.uniform(34.05, 34.15), 6),
            "longitude": round(random.uniform(-118.25, -118.20), 6)
        },
        "severity": random.choice(["Low", "Medium", "High"]),
        "emergency_services": {
            "fire_trucks_dispatched": random.randint(0, 5),
            "ambulances_dispatched": random.randint(0, 3),
            "police_units_dispatched": random.randint(0, 4),
            "drone_surveillance_used": random.choice([True, False])
        },
        "response_time_minutes": random.randint(5, 20),
        "casualties": {
            "injured": random.randint(0, 5),
            "fatalities": random.randint(0, 2)
        },
        "damage_assessment": {
            "estimated_damage_usd": random.randint(10000, 1000000),
            "buildings_affected": random.randint(1, 5),
            "evacuations_ordered": random.choice([True, False])
        }
    }

# Run continuously every 30 minutes
if __name__ == "__main__":
    while True:
        data = generate_public_services_data()
        print(json.dumps(data, indent=4))
        time.sleep(5)  # Sleep for 30 minutes before generating new data
