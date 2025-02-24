import sys
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pytz  # For timezone conversion

# Initialize Glue Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# S3 Client
S3_CLIENT = boto3.client("s3")

# S3 Buckets
RAW_BUCKET = "citysim-raw-data-bucket"
PROCESSED_BUCKET = "citysim-processed-data-bucket"

# Data Topics
RAW_TOPICS = ["traffic", "utilities", "public_services", "environment", "iot_sensors"]
ANALYTICS_TOPICS = ["traffic_analytics", "utilities_analytics", "public_services_analytics", "environment_analytics", "iot_analytics"]

# Get Current Timestamp in UTC
now = datetime.utcnow()
current_timestamp = now.strftime("%Y-%m-%dT%H-%M")
start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
last_hour = now - timedelta(hours=1)  # Get last hour timestamp

# Convert current time to PST
pst = pytz.timezone('America/Los_Angeles')
now_pst = now.replace(tzinfo=pytz.utc).astimezone(pst)

# Check if it's 11:30 PM PST
is_11pm_pst = now_pst.hour == 23 and now_pst.minute == 30

### **🔹 Function: Fetch All Files Since Given Time**
def get_files_since_time(bucket, prefix, since_time):
    """Fetch all files modified since the given timestamp."""
    files = []
    continuation_token = None

    while True:
        list_kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token

        response = S3_CLIENT.list_objects_v2(**list_kwargs)

        if "Contents" in response:
            for obj in response["Contents"]:
                last_modified = obj["LastModified"].replace(tzinfo=None)
                if last_modified >= since_time:
                    file_path = f"s3://{bucket}/{obj['Key']}"
                    files.append(file_path)

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    return files

### **🔹 Function: Cleanup S3 Folder**
def cleanup_s3_folder(bucket, prefix):
    """Remove unnecessary S3 folders created by Spark (_SUCCESS, _temporary, etc.)."""
    response = S3_CLIENT.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith("_SUCCESS") or obj["Key"].endswith("/"):
                S3_CLIENT.delete_object(Bucket=bucket, Key=obj["Key"])

### **🔹 Function: Move Final File from Temporary Directory**
def move_final_file(bucket, temp_prefix, final_prefix):
    """Move the actual output file from a Spark-generated temp directory to the final location."""
    response = S3_CLIENT.list_objects_v2(Bucket=bucket, Prefix=temp_prefix)
    
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".txt") or obj["Key"].endswith(".json"):
                temp_file = obj["Key"]
                final_file = f"{final_prefix}{temp_file.split('/')[-1]}"

                # Copy the file to the final destination
                copy_source = {"Bucket": bucket, "Key": temp_file}
                S3_CLIENT.copy_object(CopySource=copy_source, Bucket=bucket, Key=final_file)

                # Delete the temp file after moving
                S3_CLIENT.delete_object(Bucket=bucket, Key=temp_file)

                print(f"✅ Moved {temp_file} → {final_file}")

    # Cleanup leftover folders
    cleanup_s3_folder(bucket, temp_prefix)

### **🔹 Function: Process and Merge Data (Generic)**
def process_and_merge(bucket, topics, is_raw=True):
    data_type = "raw" if is_raw else "processed"
    print(f"📌 Processing {data_type} data for merging...")

    for topic in topics:
        topic_prefix = f"{topic}/"

        # Fetch last hour files for hourly merge
        hourly_files = get_files_since_time(bucket, topic_prefix, last_hour)
        
        # Fetch all files since start of the day for cumulative merge
        cumulative_files = get_files_since_time(bucket, topic_prefix, start_of_day)

        if hourly_files:
            df_hourly = spark.read.text(hourly_files)
            temp_hourly_path = f"temp/{topic}_hourly/"
            final_hourly_path = f"merged/hourly/{topic}_{current_timestamp}.txt"

            df_hourly.coalesce(1).write.mode("overwrite").text(f"s3://{bucket}/{temp_hourly_path}")
            move_final_file(bucket, temp_hourly_path, final_hourly_path)

        if cumulative_files and is_11pm_pst:  # Only run cumulative merge at 11:00 PM PST
            df_cumulative = spark.read.text(cumulative_files)
            temp_cumulative_path = f"temp/{topic}_cumulative/"
            final_cumulative_path = f"merged/cumulative/{topic}_{current_timestamp}.txt"

            # Always read previous cumulative file and append new data
            cumulative_file_path = f"merged/cumulative/{topic}_cumulative.txt"
            existing_files = get_files_since_time(bucket, f"merged/cumulative/{topic}_", start_of_day)
            
            if existing_files:
                df_existing = spark.read.text(existing_files)
                df_cumulative = df_existing.union(df_cumulative)

            df_cumulative.coalesce(1).write.mode("overwrite").text(f"s3://{bucket}/{temp_cumulative_path}")
            move_final_file(bucket, temp_cumulative_path, final_cumulative_path)

            # Convert cumulative merged file to JSON
            try:
                json_temp_path = f"temp/{topic}_cumulative_json/"
                json_final_path = f"merged/cumulative/{topic}_{current_timestamp}.json"
                df_merged_json = spark.read.json(df_cumulative.rdd.map(lambda row: row[0]))
                df_merged_json.coalesce(1).write.mode("overwrite").json(f"s3://{bucket}/{json_temp_path}")
                move_final_file(bucket, json_temp_path, json_final_path)
            except Exception as e:
                print(f"❌ Error converting cumulative file to JSON: {e}")

### **🚀 Run Merge Jobs**
try:
    process_and_merge(RAW_BUCKET, RAW_TOPICS, is_raw=True)
    process_and_merge(PROCESSED_BUCKET, ANALYTICS_TOPICS, is_raw=False)
except Exception as e:
    print(f"❌ Error merging files: {e}")

job.commit()