#!/usr/bin/env python3
"""
E-commerce Streaming with PySpark and PostgreSQL
- Basic streaming logic with minimal transformations
"""

import os
import time
import threading
import logging
from pyspark.sql.functions import col,monotonically_increasing_id, to_timestamp,udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import uuid


# Setup logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger("stream_processor")

log = setup_logging()

# Configs (using environment variables with defaults)
DATA_DIR = os.getenv("DATA_FOLDER", "ecommerce_data")
ARCHIVE_DIR = os.getenv("ARCHIVE_FOLDER", "archived")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_FOLDER", "checkpoints")

#DB_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://db:5432/ecommerce_db")
DB_URL = "jdbc:postgresql://172.21.0.2:5432/ecommerce_db"
DB_TABLE = os.getenv("TABLE_NAME", "ecommerce_events")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")

# Schema for incoming CSV events
event_schema = StructType([
    StructField("event_id", StringType(), True),  # Keeping event_id as StringType for compatibility
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True)
])  

# UDF for generating UUIDs
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# Function to process each batch and write to PostgreSQL (with minimal transformations)

def process_batch(df, batch_id):
    log.info(f"Processing batch {batch_id} with {df.count()} records")

    # Cast all fields appropriately
    df = df.withColumn("event_key", uuid_udf()) \
       .withColumn("event_id", col("event_id").cast("string")) \
       .withColumn("user_id", col("user_id").cast("string")) \
       .withColumn("product_id", col("product_id").cast("string")) \
       .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("event_type", col("event_type")) \
       .withColumn("product_name", col("product_name")) \
       .withColumn("category", col("category")) \
       .withColumn("price", col("price").cast("decimal(10,2)")) \
       .withColumn("quantity", col("quantity").cast("int"))

     # Drop duplicates based on event_id (or other unique identifier)
    df = df.dropDuplicates(["event_id"])

    # Fill null values in quantity column with 0
    df = df.fillna({"quantity": 0})

    # Repartition the data based on event_type into 2 partitions (since you have 2 types of events)
    df = df.repartition(2, "event_type")  # 2 partitions for 2 event types

    df = df.select(
    "event_key", "event_id", "user_id", "product_id", "event_time",
    "event_type", "product_name", "category", "price", "quantity"
    )


    # Write to PostgreSQL
    df.write.jdbc(
        url=DB_URL,
        table=DB_TABLE,
        mode="append",
        properties={
            "user": DB_USER,
            "password": DB_PASS,
            "driver": DB_DRIVER,
            "stringtype": "unspecified"
        }
    )
    log.info(f"Batch {batch_id} successfully written to DB")


# Main streaming setup and execution
def start_stream():
    # Ensure directories exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EcommerceStreamProcessor") \
        .config("spark.jars", "postgresql-42.7.5.jar") \
        .getOrCreate()
    
    log.info("Spark session initialized")

    # Define the streaming data source
    df_stream = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(event_schema) \
        .load(DATA_DIR)
    
    
    # PostgreSQL sink using foreachBatch
    db_query = df_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .option("cleanSource", "archive") \
        .option("archiveLocation", ARCHIVE_DIR) \
        .start()
    log.info("PostgreSQL sink started")
    
    # Await termination for both queries
    #console_query.awaitTermination()
    db_query.awaitTermination()

# Start background data generation (if applicable)
def generate_data_forever():
    import data_generator as dg  # Assuming you have a data generator module
    while True:
        dg.generate_events(num_events=5)  # Generating mock events
        time.sleep(5)

if __name__ == "__main__":
    log.info("Starting E-commerce stream processing pipeline")
    
    # Start data generation in a background thread
    generator_thread = threading.Thread(target=generate_data_forever, daemon=True)
    generator_thread.start()
    
    # Start streaming process
    start_stream()
