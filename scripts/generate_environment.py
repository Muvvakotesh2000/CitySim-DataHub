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

city_names = load_data("city_names.txt")
us_states = load_data("us_states.txt")

def generate_environment_data():
    return {
        "sensor_id": f"ENV-{random.randint(10000, 99999)}",
        "timestamp": (datetime.utcnow() + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "location": {
            "latitude": round(random.uniform(34.05, 34.15), 6),
            "longitude": round(random.uniform(-118.25, -118.20), 6),
            "city": random.choice(city_names),
            "state": random.choice(us_states)
        },
        "air_quality": {
            "AQI": random.randint(50, 200),
            "PM2_5": random.randint(10, 80),
            "PM10": random.randint(20, 120),
            "CO_ppm": round(random.uniform(0.1, 2.5), 2),
            "NO2_ppm": round(random.uniform(0.01, 0.1), 3),
            "VOC_ppb": random.randint(50, 250),
            "SO2_ppm": round(random.uniform(0.01, 0.5), 3),
            "O3_ppm": round(random.uniform(0.01, 0.2), 3)
        },
        "weather_conditions": {
            "temperature_celsius": round(random.uniform(10, 40), 2),
            "humidity_percent": random.randint(30, 90),
            "wind_speed_kmh": round(random.uniform(0, 50), 2),
            "precipitation_mm": round(random.uniform(0, 50), 2),
            "ultraviolet_index": random.randint(0, 10),
            "visibility_km": round(random.uniform(1, 10), 2)
        },
        "alerts": {
            "smog_alert": random.choice([True, False]),
            "storm_warning": random.choice([True, False]),
            "heatwave_alert": random.choice([True, False]),
            "air_quality_alert": random.choice([True, False])
        }
    }

if __name__ == "__main__":
    while True:
        data = generate_environment_data()
        print(json.dumps(data, indent=4))
        time.sleep(5)  # Generate new data every 30 minutes
