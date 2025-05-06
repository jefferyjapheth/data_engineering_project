
import sys
import os
# Add project root to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from scripts.kafka_producer import create_kafka_producer
from utils.logger import setup_logger
from data_generator import stream_heart_rate_data, process_and_stream_data

# Set up logging
log = setup_logger("stream_processor")

# Kafka and DB configuration
kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9093')
DB_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://172.22.0.2:5432/heartbeat_db")
DB_TABLE = os.getenv("TABLE_NAME", "sports_heart_data")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_FOLDER", "checkpoints")

# JSON Schema for Kafka values
json_schema = StructType([
    StructField("athlete_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("heart_rate", IntegerType(), False),
    StructField("activity_status", StringType(), False),
])

# Function to process each batch of streamed data
def process_batch(df, batch_id):
    log.info(f"Processing batch {batch_id} with {df.count()} records")
    df.write.jdbc(
        url=DB_URL,
        table=DB_TABLE,
        mode="append",
        properties={
            "user": DB_USER,
            "password": DB_PASS,
            "driver": DB_DRIVER,
        }
    )
    log.info(f"Batch {batch_id} successfully written to DB")

# Start Spark Streaming session
def start_stream():
    spark = SparkSession.builder \
    .appName("HeartRateStreamProcessor") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", "postgresql-42.7.5.jar") \
    .getOrCreate()

    log.info("Spark session started")

    # Kafka stream configuration
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", "sports_heart_rate") \
        .option("startingOffsets", "latest") \
        .load()

    # Parsing and transforming the Kafka stream
    df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

    # Write stream to PostgreSQL
    db_query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    df_parsed.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()


    log.info("Streaming query started")
    db_query.awaitTermination()

# Background thread to produce data and send it to Kafka
def background_data_producer():
    send_to_kafka = create_kafka_producer(kafka_broker)
    while True:
        records = stream_heart_rate_data(num_athletes=10, num_records=10)
        process_and_stream_data(records, send_to_kafka, "sports_heart_rate")
        time.sleep(5)

# Main function to run the entire pipeline
if __name__ == "__main__":
    log.info("Starting Heart Rate Streaming Pipeline")

    # Start data producer in the background (as a separate thread)
    producer_thread = threading.Thread(target=background_data_producer, daemon=True)
    producer_thread.start()

    # Start the Spark streaming process
    start_stream()
