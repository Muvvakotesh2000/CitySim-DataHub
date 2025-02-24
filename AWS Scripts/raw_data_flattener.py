import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import boto3

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Input and Output Paths
RAW_BUCKET = "s3://citysim-raw-data-bucket/"
OUTPUT_BUCKET = "s3://citysim-etl-data/"
TEMP_BUCKET = "s3://citysim-etl-temp/"

# Initialize S3 client
s3_client = boto3.client('s3')

# Function to Flatten Nested JSON
def flatten_df(nested_df):
    flat_cols = [F.col(col) for col in nested_df.columns if not isinstance(nested_df.schema[col].dataType, F.StructType)]
    nested_cols = [(col, F.col(col + ".*")) for col in nested_df.columns if isinstance(nested_df.schema[col].dataType, F.StructType)]

    while nested_cols:
        col_name, col_struct = nested_cols.pop(0)
        flat_cols.extend([F.col(f"{col_name}.{field.name}").alias(f"{col_name}_{field.name}") for field in col_struct.schema.fields])
    
    return nested_df.select(flat_cols)

# Function to clean and transform the data
def clean_and_transform(row):
    cleaned_row = {}
    for key, value in row.asDict().items():
        # Skip processing for 'ingestion_time' and 'topic' columns
        if key in ["ingestion_time", "topic"]:
            cleaned_row[key] = value
        elif isinstance(value, str):
            # Remove unwanted characters
            cleaned_value = value.replace("{", "").replace("}", "").replace("\\", "").replace("/", "").replace('"', "")
            # Split into key-value pairs
            key_value_pairs = cleaned_value.split(",")
            for pair in key_value_pairs:
                if ":" in pair:
                    k, v = pair.split(":", 1)
                    cleaned_row[k.strip()] = v.strip()
        else:
            cleaned_row[key] = value
    return cleaned_row

# Function to delete S3 folder
def delete_s3_folder(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
        print(f"Deleted folder: s3://{bucket}/{prefix}")

# Processing Each Folder
folders = ["environment", "iot_sensors", "public_services", "traffic", "utilities"]

for folder in folders:
    print(f"Processing {folder} data...")

    # Read Raw JSON files
    df = spark.read.json(f"{RAW_BUCKET}{folder}/")

    # Ensure JSON is properly loaded
    if df.isEmpty():
        print(f"No data found in {folder}, skipping...")
        continue

    # Flatten JSON structure
    flattened_df = flatten_df(df)

    # Save as CSV
    output_path = f"{OUTPUT_BUCKET}{folder}/"
    flattened_df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Saved flattened {folder} data to {output_path}")

    # Read the saved CSV file for further processing
    csv_df = spark.read.csv(output_path, header=True)

    # Clean and transform each row
    cleaned_rdd = csv_df.rdd.map(clean_and_transform)

    # Convert RDD back to DataFrame
    cleaned_df = spark.createDataFrame(cleaned_rdd)

    # Save cleaned data to temporary bucket
    temp_output_path = f"{TEMP_BUCKET}{folder}/"
    cleaned_df.write.mode("overwrite").option("header", "true").csv(temp_output_path)
    print(f"Saved cleaned {folder} data to {temp_output_path}")

    # Read the cleaned CSV file for final processing
    final_df = spark.read.csv(temp_output_path, header=True)

    # Save final cleaned data to output bucket
    final_output_path = f"{OUTPUT_BUCKET}{folder}_final/"
    final_df.write.mode("overwrite").option("header", "true").csv(final_output_path)
    print(f"Saved final cleaned {folder} data to {final_output_path}")

    # Delete temporary files
    delete_s3_folder(TEMP_BUCKET.replace("s3://", "").rstrip("/"), f"{folder}/")
    print(f"Deleted temporary files for {folder}")

# Complete Glue Job
job.commit()