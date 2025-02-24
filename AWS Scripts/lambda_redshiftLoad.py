import boto3
import psycopg2
import json

# AWS S3 Configuration
S3_BUCKET = "citysim-etl-data"
REDSHIFT_ROLE = "*"

# Redshift Configuration
REDSHIFT_HOST = "your-redshift-cluster.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "your_database"
REDSHIFT_USER = "your_user"
REDSHIFT_PASSWORD = "your_password"

# List of tables and S3 folder mapping
TABLES = {
    "environment_data": "environment",
    "iot_sensors_data": "iot_sensors",
    "public_services_data": "public_services",
    "traffic_data": "traffic",
    "utilities_data": "utilities"
}

def lambda_handler(event, context):
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT
        )
        cursor = conn.cursor()
        
        for table, folder in TABLES.items():
            copy_sql = f"""
            COPY {table}
            FROM 's3://{S3_BUCKET}/{folder}/'
            IAM_ROLE '{REDSHIFT_ROLE}'
            FORMAT AS CSV
            IGNOREHEADER 1
            DELIMITER ',';
            """
            print(f"Executing COPY for {table}")
            cursor.execute(copy_sql)
            conn.commit()

        cursor.close()
        conn.close()
        return {"status": "success", "message": "Redshift data load completed"}
    
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}
