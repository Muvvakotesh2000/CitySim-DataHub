import boto3
import json
import os
from aws_config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_RAW_BUCKET, S3_PROCESSED_BUCKET

TEMP_DIR = "E:/Kotesh/Projects/CitySim/CitySimCloud/tmp/"

# Ensure the directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

def get_s3_client():
    """Initialize and return an S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

def upload_to_s3(batch_df, batch_id, s3_bucket, s3_prefix, filename):
    """Upload JSON data to S3 efficiently."""
    try:
        s3_client = get_s3_client()

        temp_path = os.path.join(TEMP_DIR, f"{filename}_{batch_id}" if batch_id else filename)

        # Save DataFrame as JSON in one partition
        batch_df.coalesce(1).write.mode("overwrite").json(temp_path)

        # Get JSON file
        json_files = [file for file in os.listdir(temp_path) if file.endswith(".json")]

        if json_files:
            json_file_path = os.path.join(temp_path, json_files[0])
            with open(json_file_path, "rb") as file:
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=f"{s3_prefix}/{filename}",
                    Body=file,
                    ContentType="application/json"
                )
            print(f"Data written to S3: s3://{s3_bucket}/{s3_prefix}/{filename}")

        # Clean up
        for file in os.listdir(temp_path):
            os.remove(os.path.join(temp_path, file))
        os.rmdir(temp_path)

    except Exception as e:
        print(f"Error uploading to S3: {e}")
