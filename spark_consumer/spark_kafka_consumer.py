from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, to_timestamp, current_timestamp, sum, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from s3_writer import upload_to_s3
from aws_config import S3_RAW_BUCKET, S3_PROCESSED_BUCKET
import os
from pyspark.sql.functions import hour
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPICS = ["traffic", "utilities", "public_services", "environment", "iot_sensors"]

# Define schema for Traffic Data
traffic_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True)
    ]), True),
    StructField("traffic_conditions", StructType([
        StructField("congestion_level", StringType(), True),
        StructField("average_speed_kmh", IntegerType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("accidents", StringType(), True)  # Stored as JSON string
    ]), True),
    StructField("weather_conditions", StructType([
        StructField("temperature_celsius", FloatType(), True),
        StructField("visibility_km", FloatType(), True),
        StructField("precipitation_mm", FloatType(), True)
    ]), True),
    StructField("traffic_management", StructType([
        StructField("traffic_lights_adjusted", BooleanType(), True),
        StructField("alternate_routes_suggested", StringType(), True)  # Stored as JSON string
    ]), True)
])

# Define schema for Utilities Data
utilities_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("building", StructType([
        StructField("name", StringType(), True),
        StructField("building_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True)
        ]), True)
    ]), True),
    StructField("energy_consumption", StructType([
        StructField("electricity_kwh", IntegerType(), True),
        StructField("gas_m3", IntegerType(), True),
        StructField("renewable_energy_percent", FloatType(), True),
        StructField("power_outage_reported", BooleanType(), True)
    ]), True),
    StructField("water_usage", StructType([
        StructField("liters_used", IntegerType(), True),
        StructField("leak_detected", BooleanType(), True),
        StructField("contamination_detected", BooleanType(), True)
    ]), True),
    StructField("waste_collection", StructType([
        StructField("bins_collected", IntegerType(), True),
        StructField("bin_fill_level_percent", IntegerType(), True),
        StructField("recycling_percentage", IntegerType(), True),
        StructField("hazardous_waste_detected", BooleanType(), True)
    ]), True)
])

# Define schema for Public Services Data
public_services_schema = StructType([
    StructField("incident_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("reported_by", StringType(), True),
    StructField("location", StructType([
        StructField("building", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True)
    ]), True),
    StructField("severity", StringType(), True),
    StructField("emergency_services", StructType([
        StructField("fire_trucks_dispatched", IntegerType(), True),
        StructField("ambulances_dispatched", IntegerType(), True),
        StructField("police_units_dispatched", IntegerType(), True),
        StructField("drone_surveillance_used", BooleanType(), True)
    ]), True),
    StructField("response_time_minutes", IntegerType(), True),
    StructField("casualties", StructType([
        StructField("injured", IntegerType(), True),
        StructField("fatalities", IntegerType(), True)
    ]), True),
    StructField("damage_assessment", StructType([
        StructField("estimated_damage_usd", IntegerType(), True),
        StructField("buildings_affected", IntegerType(), True),
        StructField("evacuations_ordered", BooleanType(), True)
    ]), True)
])

# Define schema for Environmental Data
environment_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("air_quality", StructType([
        StructField("AQI", IntegerType(), True),
        StructField("PM2_5", IntegerType(), True),
        StructField("PM10", IntegerType(), True),
        StructField("CO_ppm", FloatType(), True),
        StructField("NO2_ppm", FloatType(), True),
        StructField("VOC_ppb", IntegerType(), True),
        StructField("SO2_ppm", FloatType(), True),
        StructField("O3_ppm", FloatType(), True)
    ]), True),
    StructField("weather_conditions", StructType([
        StructField("temperature_celsius", FloatType(), True),
        StructField("humidity_percent", IntegerType(), True),
        StructField("wind_speed_kmh", FloatType(), True),
        StructField("precipitation_mm", FloatType(), True),
        StructField("ultraviolet_index", IntegerType(), True),
        StructField("visibility_km", FloatType(), True)
    ]), True),
    StructField("alerts", StructType([
        StructField("smog_alert", BooleanType(), True),
        StructField("storm_warning", BooleanType(), True),
        StructField("heatwave_alert", BooleanType(), True),
        StructField("air_quality_alert", BooleanType(), True)
    ]), True)
])

# Define schema for IoT Sensor Data
iot_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("building", StructType([
        StructField("name", StringType(), True),
        StructField("building_id", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("room_number", StringType(), True)
    ]), True),
    StructField("occupancy", StructType([
        StructField("people_count", IntegerType(), True),
        StructField("capacity_limit", IntegerType(), True),
        StructField("motion_detected", BooleanType(), True)
    ]), True)
])

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.sql.streaming.checkpointLocation", "logs/") \
    .config("spark.driver.memory", "10g")\
    .config("spark.executor.memory", "6g")\
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()


# Read from Kafka topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ",".join(KAFKA_TOPICS)) \
    .option("startingOffsets", "latest") \
    .load()


# Parse JSON data from Kafka
parsed_df = df.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)").withColumn("ingestion_time", current_timestamp())

from datetime import datetime

def write_raw_to_s3(batch_df, batch_id):
    """Write raw Kafka data to S3 with structured subfolders per topic."""
    if batch_df.count() > 0:
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Extract the topic dynamically
        topic_df = batch_df.select("topic").distinct().collect()
        for row in topic_df:
            topic = row["topic"]
            topic_df_filtered = batch_df.filter(col("topic") == topic)

            file_with_timestamp = f"{topic}_{timestamp_str}"
            s3_path = f"{S3_RAW_BUCKET}"
            
            # Upload per topic
            upload_to_s3(topic_df_filtered, batch_id, s3_path, f"{topic}", file_with_timestamp)
            print(f"Processed batch {batch_id}: {file_with_timestamp} saved to {s3_path}")



parsed_df.writeStream.foreachBatch(write_raw_to_s3) \
    .trigger(processingTime="5 minutes") \
    .outputMode("append") \
    .start()


# Traffic Analytics
traffic_df = parsed_df.filter(col("topic") == "traffic") \
    .select(from_json(col("value"), traffic_schema).alias("data")) \
    .select(
        "data.event_id",
        "data.timestamp",
        col("data.location.city").alias("city"),  #Extract city
        col("data.traffic_conditions.congestion_level").alias("congestion_level"),  # Extract congestion level
        col("data.traffic_conditions.average_speed_kmh").alias("average_speed_kmh"),
        col("data.traffic_conditions.vehicle_count").alias("vehicle_count"),
        col("data.traffic_conditions.accidents").alias("accidents"))\
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

#Traffic Aggregations
traffic_speed_analysis = traffic_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "city", "congestion_level") \
    .avg("average_speed_kmh")

traffic_vehicle_count = traffic_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "city") \
    .sum("vehicle_count")

traffic_accidents = traffic_df.withWatermark("timestamp", "5 minutes")\
    .groupBy(window("timestamp", "5 minutes"), "city")\
    .count().alias("accidents")



# Utilities Analytics
utilities_df = parsed_df.filter(col("topic") == "utilities") \
    .select(from_json(col("value"), utilities_schema).alias("data")) \
    .select(
        "data.event_id",
        "data.timestamp",
        col("data.building.type").alias("building_type"),  #Extract building type
        col("data.building.location.latitude").alias("latitude"),  # Extract latitude
        col("data.building.location.longitude").alias("longitude"),  # Extract longitude
        col("data.energy_consumption.electricity_kwh").alias("electricity_kwh"),
        col("data.energy_consumption.gas_m3").alias("gas_m3"),
        col("data.waste_collection.hazardous_waste_detected").alias("hazardous_waste_detected")  # Extract hazardous waste detection
    ) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

#  Utilities Aggregations
utilities_electricity_usage = utilities_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "building_type") \
    .sum("electricity_kwh")

utilities_gas_usage = utilities_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "building_type") \
    .sum("gas_m3")

hazardous_waste_analysis = utilities_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "latitude", "longitude") \
    .count()



# Public Services Analytics
public_services_df = parsed_df.filter(col("topic") == "public_services") \
    .select(from_json(col("value"), public_services_schema).alias("data")) \
    .select(
        "data.incident_id",
        "data.timestamp",
        col("data.incident_type").alias("incident_type"),  # Extract incident type
        col("data.severity").alias("severity"),
        col("data.response_time_minutes").alias("response_time_minutes"),
        col("data.casualties.injured").alias("injured"),
        col("data.casualties.fatalities").alias("fatalities")
    ) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Public Services Aggregations (Now using extracted fields)
public_services_response_time = public_services_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "incident_type") \
    .avg("response_time_minutes")

casualties_per_incident = public_services_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "incident_type") \
    .sum("injured", "fatalities")




# Environmental Data Analytics
environment_df = parsed_df.filter(col("topic") == "environment") \
    .select(from_json(col("value"), environment_schema).alias("data")) \
    .select(
        "data.sensor_id",
        "data.timestamp",
        col("data.location.city").alias("city"),  # Extract city
        col("data.location.state").alias("state"),  # Extract state
        col("data.air_quality.AQI").alias("AQI"),
        col("data.weather_conditions.temperature_celsius").alias("temperature_celsius"),
        col("data.alerts.storm_warning").alias("storm_warning")  #  Extract storm warning
    ) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Environment Aggregations
avg_aqi_per_city = environment_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "city") \
    .avg("AQI")

weather_air_quality_correlation = environment_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "temperature_celsius") \
    .avg("AQI")

# Storm Warnings Per State (Now using extracted `state` field)
storm_warnings_per_state = environment_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "state") \
    .count()


# IoT Sensor Data Analytics
from pyspark.sql.functions import hour

# Extract IoT Sensor Data
iot_df = parsed_df.filter(col("topic") == "iot_sensors") \
    .select(from_json(col("value"), iot_schema).alias("data")) \
    .select(
        "data.sensor_id",
        "data.timestamp",
        col("data.building.name").alias("building_name"),  # Extract building name
        col("data.building.room_number").alias("room_number"),  # Extract room number
        col("data.occupancy.people_count").alias("people_count"),
        col("data.occupancy.capacity_limit").alias("capacity_limit"),
        col("data.occupancy.motion_detected").alias("motion_detected")  # Extract motion detection
    ) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# IoT Aggregations
avg_occupancy_per_building = iot_df.withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "building_name") \
    .avg("people_count")

over_capacity_rooms = iot_df.filter(col("people_count") > col("capacity_limit")) \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "building_name", "room_number") \
    .count()

# Motion Detected at Night (Correctly filters 10 PM - 12 AM using `hour()`)
motion_detected_night = iot_df.withWatermark("timestamp", "5 minutes") \
    .filter(hour(col("timestamp")).between(22, 23) | (hour(col("timestamp")) == 0)) \
    .groupBy(window("timestamp", "5 minutes"), "building_name", "room_number") \
    .count()


# Streaming analytics: Aggregating data in real-time
traffic_analytics = traffic_df.withWatermark("timestamp", "5 minutes").groupBy("congestion_level",window("timestamp", "5 minutes")).count()
utilities_analytics = utilities_df.withWatermark("timestamp", "5 minutes").groupBy("building_type", window("timestamp", "5 minutes")).avg("electricity_kwh")
public_services_analytics = public_services_df.withWatermark("timestamp", "5 minutes").groupBy("incident_type", window("timestamp", "5 minutes")).avg("response_time_minutes")
environment_analytics = environment_df.withWatermark("timestamp", "5 minutes").groupBy("AQI", window("timestamp", "5 minutes")).count()
iot_analytics = iot_df.withWatermark("timestamp", "5 minutes").groupBy("building_name",window("timestamp", "5 minutes")).avg("people_count")

def write_analytics_to_s3(batch_df, batch_id, filename):
    if batch_df.count() > 0:
        # Generate timestamp
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Create a filename with timestamp
        file_with_timestamp = f"{filename}_{timestamp_str}"
        
        # Upload to S3
        upload_to_s3(batch_df, batch_id, S3_PROCESSED_BUCKET, f'{filename}', file_with_timestamp)
        
        print(f"Processing batch {batch_id} for {file_with_timestamp}...")

    
# Send Processed Analytics to Local First
traffic_analytics.writeStream.foreachBatch(lambda df, epoch_id: write_analytics_to_s3(df, epoch_id, "traffic_analytics")).trigger(processingTime="5 minutes").outputMode("update").start()
utilities_analytics.writeStream.foreachBatch(lambda df, epoch_id: write_analytics_to_s3(df, epoch_id, "utilities_analytics")).trigger(processingTime="5 minutes").outputMode("update").start()
public_services_analytics.writeStream.foreachBatch(lambda df, epoch_id: write_analytics_to_s3(df, epoch_id, "public_services_analytics")).trigger(processingTime="5 minutes").outputMode("update").start()
environment_analytics.writeStream.foreachBatch(lambda df, epoch_id: write_analytics_to_s3(df, epoch_id, "environment_analytics")).trigger(processingTime="5 minutes").outputMode("update").start()
iot_analytics.writeStream.foreachBatch(lambda df, epoch_id: write_analytics_to_s3(df, epoch_id, "iot_analytics")).trigger(processingTime="5 minutes").outputMode("update").start()


# Output results to console in real-time
traffic_analytics.writeStream.outputMode("update").format("console").start()
utilities_analytics.writeStream.outputMode("update").format("console").start()
public_services_analytics.writeStream.outputMode("update").format("console").start()
environment_analytics.writeStream.outputMode("update").format("console").start()
iot_analytics.writeStream.outputMode("update").format("console").start()

# Keep the streaming job running
spark.streams.awaitAnyTermination()
