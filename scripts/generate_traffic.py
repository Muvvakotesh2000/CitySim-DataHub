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

# Load external data sources
street_names = load_data("street_names.txt")
city_names = load_data("city_names.txt")
us_states = load_data("us_states.txt")
zipcodes = load_data("zipcodes.txt")

# Define probability for accidents
ACCIDENT_PROBABILITY = 0.15  # 15% chance of an accident occurring

def generate_traffic_data():
    """Generates real-world structured traffic data with accident probability."""
    
    congestion_levels = ["Low", "Medium", "High"]
    traffic_speed = {"Low": random.randint(5, 30), "Medium": random.randint(30, 60), "High": random.randint(60, 100)}
    selected_congestion = random.choice(congestion_levels)

    traffic_event = {
        "event_id": f"TRF-{random.randint(10000, 99999)}",
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "location": {
            "street": random.choice(street_names),
            "city": random.choice(city_names),
            "state": random.choice(us_states),
            "zip_code": random.choice(zipcodes),
            "latitude": round(random.uniform(34.05, 34.15), 6),
            "longitude": round(random.uniform(-118.25, -118.20), 6)
        },
        "traffic_conditions": {
            "congestion_level": selected_congestion,
            "average_speed_kmh": traffic_speed[selected_congestion],
            "vehicle_count": random.randint(50, 500),
            "accidents": []
        },
        "weather_conditions": {
            "temperature_celsius": round(random.uniform(5, 40), 2),
            "visibility_km": round(random.uniform(0.5, 10), 2),
            "precipitation_mm": round(random.uniform(0, 10), 2)
        },
        "traffic_management": {
            "traffic_lights_adjusted": random.choice([True, False]),
            "alternate_routes_suggested": random.sample(street_names, k=2)
        }
    }

    # Add an accident if the probability threshold is met
    if random.random() < ACCIDENT_PROBABILITY:
        traffic_event["traffic_conditions"]["accidents"].append({
            "accident_id": f"ACC-{random.randint(1000, 9999)}",
            "severity": random.choice(["Minor", "Major", "Severe"]),
            "vehicles_involved": random.randint(1, 5),
            "injuries": random.randint(0, 3),
            "fatalities": random.randint(0, 1),
            "road_closure": random.choice([True, False])
        })

    return traffic_event

if __name__ == "__main__":
    while True:
        traffic_data = generate_traffic_data()
        print(json.dumps(traffic_data, indent=4))
        time.sleep(5)  # Generate new data every 30 minutes
