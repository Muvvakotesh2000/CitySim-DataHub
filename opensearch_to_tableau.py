import time
import schedule
import os
import pandas as pd
from opensearchpy import OpenSearch

# OpenSearch Configuration
OPENSEARCH_HOST = "https://search-search-citysim-opensearch-weukyzhhwovjznql3srewakzwu.us-east-1.es.amazonaws.com"
OPENSEARCH_AUTH = ("admin", "Padma#@12")  # Update with correct credentials
OPENSEARCH_INDICES = [
    "environment_analytics",
    "iot_analytics",
    "public_services_analytics",
    "traffic_analytics",
    "utilities_analytics"
]

# Schema Definitions for OpenSearch Indices
INDEX_SCHEMAS = {
    "environment_analytics": ["AQI", "count", "window_start", "window_end"],
    "iot_analytics": ["avg_people_count", "building_name", "window_start", "window_end"],
    "public_services_analytics": ["avg_response_time_minutes", "incident_type", "window_start", "window_end"],
    "traffic_analytics": ["congestion_level", "count", "window_start", "window_end"],
    "utilities_analytics": ["avg_electricity_kwh", "building_type", "window_start", "window_end"]
}

# Set directory to save CSV file
CSV_DIR = "E:\\Kotesh\\Projects\\CitySim\\CitySimCloud"
MERGED_CSV_FILE = os.path.join(CSV_DIR, "merged_opensearch_data.csv")
TEMP_CSV_FILE = os.path.join(CSV_DIR, "merged_opensearch_data_temp.csv")

if not os.path.exists(CSV_DIR):
    os.makedirs(CSV_DIR)

# Connect to OpenSearch
client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_auth=OPENSEARCH_AUTH,
    use_ssl=True,
    verify_certs=True,
)

# Function to fetch data from OpenSearch and save as a single merged CSV
def fetch_and_save_merged_csv():
    print("\n🔄 Fetching latest data from OpenSearch...")

    dataframes = []  # List to hold dataframes for merging

    for index in OPENSEARCH_INDICES:
        print(f"📡 Querying index: {index}...")

        query = {
            "size": 1000,  # Fetch latest 1000 records
            "query": {"match_all": {}},  # Get all records
            "sort": [{"window_start": "desc"}]  # Sort by most recent
        }

        try:
            response = client.search(index=index, body=query)
            schema_fields = INDEX_SCHEMAS[index]  # Get schema for this index

            data = [{field: hit["_source"].get(field, None) for field in schema_fields} for hit in response["hits"]["hits"]]

            if data:
                df = pd.DataFrame(data)

                # Ensure DataFrame has all required columns
                for col in schema_fields:
                    if col not in df.columns:
                        df[col] = None  # Fill missing columns with None values

                # Add a Table_Name column to identify the source index
                df["Table_Name"] = index

                # Add to list for merging
                dataframes.append(df)

            else:
                print(f"❌ No data found in {index}.")

        except Exception as e:
            print(f"❌ Error fetching data from {index}: {e}")

    # Merge all dataframes and save to a temporary CSV file
    if dataframes:
        merged_df = pd.concat(dataframes, ignore_index=True)
        merged_df.to_csv(TEMP_CSV_FILE, index=False)
        print(f"✅ Temporary merged data saved to {TEMP_CSV_FILE}.")

        # Replace the old file safely
        replace_merged_file()

# Function to safely replace the merged CSV file
def replace_merged_file():
    """Replaces the old merged CSV file with the new one after ensuring it's not in use by Tableau."""
    max_attempts = 10  # Number of retry attempts
    attempt = 0

    while attempt < max_attempts:
        try:
            # Remove the old file if it exists
            if os.path.exists(MERGED_CSV_FILE):
                os.remove(MERGED_CSV_FILE)

            # Rename the temporary file to the final merged file
            os.rename(TEMP_CSV_FILE, MERGED_CSV_FILE)
            print("✅ Merged CSV file successfully replaced.")
            return  # Exit function when successful

        except PermissionError:
            print(f"🔄 Tableau is using the file. Retrying in 5 seconds... (Attempt {attempt + 1}/{max_attempts})")
            time.sleep(5)  # Wait before retrying
            attempt += 1

    print("❌ Could not replace the merged CSV file. Tableau may still be using it.")

# Run every 5 minutes
schedule.every(5).minutes.do(fetch_and_save_merged_csv)

print("\n🚀 Starting OpenSearch → Single Merged CSV export...\n")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(300)  # Run every 5 minutes
