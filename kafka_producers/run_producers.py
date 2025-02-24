import subprocess
import os

# Get the absolute path of the current script (run_producers.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the correct path to the kafka_producers folder
producer_folder = os.path.join(BASE_DIR)  #Fix: Only one "kafka_producers"

# List of Kafka producer scripts
scripts = [
    "kafka_traffic_producer.py",
    "kafka_utilities_producer.py",
    "kafka_public_services_producer.py",
    "kafka_environment_producer.py",
    "kafka_iot_sensors_producer.py"
]

# Start each script in parallel
processes = []
for script in scripts:
    script_path = os.path.join(producer_folder, script)  # Fix: Ensure correct path
    if os.path.exists(script_path):  # Check if script exists before running
        print(f"Running: {script_path}")  # Debug message
        processes.append(subprocess.Popen(["python", script_path]))
    else:
        print(f"Error: {script_path} not found!")

# Wait for all scripts to finish (optional)
for process in processes:
    process.wait()
