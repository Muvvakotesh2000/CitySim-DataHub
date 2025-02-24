import subprocess
import os

# Get the absolute path of the current script (run_generators.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the correct path to the scripts folder
scripts_folder = os.path.join(BASE_DIR, "scripts")  # ✅ Ensures the correct path

# List of data generator scripts
scripts = [
    "generate_environment.py",
    "generate_iot_sensors.py",
    "generate_public_services.py",
    "generate_traffic.py",
    "generate_utilities.py"
]

# Start each script in parallel
processes = []
for script in scripts:
    script_path = os.path.join(scripts_folder, script)  # ✅ Fix: Ensures correct path
    if os.path.exists(script_path):  # ✅ Check if script exists before running
        print(f"✅ Running: {script_path}")  # Debug message
        processes.append(subprocess.Popen(["python", script_path]))
    else:
        print(f"❌ Error: {script_path} not found!")

# Wait for all scripts to finish (optional)
for process in processes:
    process.wait()
