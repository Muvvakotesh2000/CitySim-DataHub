import sys
import boto3
import json
import requests
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from requests.auth import HTTPBasicAuth

# 🔹 OpenSearch Credentials
OPENSEARCH_USER = "*"
OPENSEARCH_PASSWORD = "*"

# 🔹 OpenSearch URLs
OPENSEARCH_URL = "*"
DASHBOARDS_URL = f"{OPENSEARCH_URL}/_dashboards:5601"

# 🔹 Headers for OpenSearch Requests
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
DASHBOARD_HEADERS = {"Content-Type": "application/json", "osd-xsrf": "true"}

# 🔹 AWS S3 Client
s3 = boto3.client('s3')

# 🔹 Define S3 bucket and folders
BUCKET_NAME = "citysim-processed-data-bucket"
FOLDERS = {
    "environment_analytics_final": "environment_analytics",
    "iot_analytics_final": "iot_analytics",
    "public_services_analytics_final": "public_services_analytics",
    "traffic_analytics_final": "traffic_analytics",
    "utilities_analytics_final": "utilities_analytics"
}

# 🔹 Define different index mappings based on schema
INDEX_MAPPINGS = {
    "environment_analytics": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "AQI": {"type": "integer"},
                "count": {"type": "integer"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"}
            }
        }
    },
    "iot_analytics": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "avg_people_count": {"type": "float"},
                "building_name": {"type": "keyword"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"}
            }
        }
    },
    "public_services_analytics": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "avg_response_time_minutes": {"type": "float"},
                "incident_type": {"type": "keyword"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"}
            }
        }
    },
    "traffic_analytics": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "congestion_level": {"type": "keyword"},
                "count": {"type": "integer"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"}
            }
        }
    },
    "utilities_analytics": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "avg_electricity_kwh": {"type": "float"},
                "building_type": {"type": "keyword"},
                "window_start": {"type": "date"},
                "window_end": {"type": "date"}
            }
        }
    }
}

# 🔹 Function to Delete Existing Index
def delete_index(index_name):
    url = f"{OPENSEARCH_URL}/{index_name}"
    response = requests.delete(url, headers=HEADERS, auth=HTTPBasicAuth(OPENSEARCH_USER, OPENSEARCH_PASSWORD))
    if response.status_code in [200, 204]:
        print(f"🗑 Deleted existing index {index_name}.")
    else:
        print(f"⚠️ Failed to delete index {index_name}: {response.status_code}, {response.text}")

# 🔹 Function to Create Index with Specific Mapping
def create_index(index_name):
    url = f"{OPENSEARCH_URL}/{index_name}"
    mapping = INDEX_MAPPINGS.get(index_name, None)

    if mapping is None:
        print(f"⚠️ No mapping found for index: {index_name}. Skipping creation.")
        return

    response = requests.put(url, headers=HEADERS, data=json.dumps(mapping),
                            auth=HTTPBasicAuth(OPENSEARCH_USER, OPENSEARCH_PASSWORD))

    if response.status_code in [200, 201]:
        print(f"✅ Created new index {index_name}.")
    else:
        print(f"❌ Failed to create index {index_name}: {response.status_code}, {response.text}")

# 🔹 Function to Push Data to OpenSearch
def push_to_opensearch(data, index_name):
    url = f"{OPENSEARCH_URL}/{index_name}/_doc/"
    success_count, error_count = 0, 0

    for record in data:
        try:
            response = requests.post(url, headers=HEADERS, 
                                     data=json.dumps(record), 
                                     auth=HTTPBasicAuth(OPENSEARCH_USER, OPENSEARCH_PASSWORD))

            if response.status_code in [200, 201]:
                success_count += 1
            else:
                error_count += 1
                print(f"⚠️ Error pushing record to OpenSearch: {response.text}")

        except Exception as e:
            print(f"❌ JSON Serialization Error: {str(e)}")

    print(f"📊 Successfully pushed {success_count} records to OpenSearch ({index_name}).")
    if error_count > 0:
        print(f"⚠️ Failed to push {error_count} records.")

# 🔹 Initialize Spark and Glue context
spark = SparkSession.builder.appName("S3ToOpenSearch").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# 🔹 Process each `_final` folder (only one file should exist)
for final_folder, index_name in FOLDERS.items():
    print(f"🚀 Processing {final_folder}...")

    # Delete and recreate index
    delete_index(index_name)
    create_index(index_name)

    # List files in the final folder
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{final_folder}/")
    if "Contents" not in response or len(response["Contents"]) == 0:
        print(f"⚠️ No files found in {final_folder}, skipping...")
        continue

    # Get the only JSON file in the folder
    latest_file = response["Contents"][0]["Key"]
    print(f"📂 Processing file: {latest_file}")

    s3_path = f"s3://{BUCKET_NAME}/{latest_file}"

    # Read JSON directly
    df = spark.read.json(s3_path)

    # Convert to JSON format
    json_data = [row.asDict() for row in df.collect()]

    # Push data to OpenSearch
    push_to_opensearch(json_data, index_name)

print("✅ Glue Job Execution Completed.")