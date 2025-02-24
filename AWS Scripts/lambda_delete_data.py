import boto3

# List of S3 bucket names
BUCKETS = [
    "citysim-etl-temp",
    "citysim-glue-scripts",
    "citysim-processed-data-bucket",
    "citysim-raw-data-bucket"
]

s3_client = boto3.client("s3")

def delete_all_files_from_bucket(bucket_name):
    """Deletes all objects inside an S3 bucket."""
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if "Contents" in objects:
            delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
            
            # Delete the objects
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
            print(f"Deleted {len(delete_keys)} objects from {bucket_name}.")
        else:
            print(f"No objects found in {bucket_name}.")
    
    except Exception as e:
        print(f"Error deleting files from {bucket_name}: {str(e)}")

def lambda_handler(event, context):
    """AWS Lambda function handler."""
    for bucket in BUCKETS:
        delete_all_files_from_bucket(bucket)
    
    return {
        "statusCode": 200,
        "message": "All files deleted from specified buckets."
    }
