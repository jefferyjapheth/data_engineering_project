import sys
import os
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Add project root to Python path for proper imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import from scripts and utils
from scripts.kafka_producer import create_kafka_producer
from scripts.data_generator import stream_heart_rate_data, process_and_stream_data
from utils.logger import setup_logger

# Set up logging
log = setup_logger("stream_processor")

# === Load Environment Variables ===
KAFKA_BROKER_DOCKER = os.getenv("KAFKA_BROKER_DOCKER", "kafka:29092")
KAFKA_BROKER_HOST = os.getenv("KAFKA_BROKER_HOST", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeats")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://db:5432/heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Amalitech")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")
TABLE_NAME = os.getenv("TABLE_NAME", "ath_heartbeats")
CHECKPOINT_FOLDER = os.getenv("CHECKPOINT_FOLDER", "/tmp/spark_checkpoint")
RUNNING_IN_DOCKER = os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"

# Override checkpoint folder on Windows
if os.name == 'nt' and CHECKPOINT_FOLDER.startswith("/"):
    CHECKPOINT_FOLDER = os.path.join(os.getcwd(), "spark_checkpoint")

# Determine correct Kafka broker
kafka_broker = KAFKA_BROKER_DOCKER if RUNNING_IN_DOCKER else KAFKA_BROKER_HOST

# === Define JSON Schema ===
json_schema = StructType([
    StructField("athlete_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("heart_rate", IntegerType(), False),
    StructField("activity_status", StringType(), False),
])

# === Spark Processing Function ===
def process_batch(df, batch_id):
    log.info(f"Processing batch {batch_id} with {df.count()} records")
    if df.count() > 0:
        log.info("Writing to database...")
        try:
            df.write.jdbc(
                url=POSTGRES_URL,
                table=TABLE_NAME,
                mode="append",
                properties={
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD,
                    "driver": POSTGRES_DRIVER,
                }
            )
            log.info(f"Batch {batch_id} successfully written to DB")
        except Exception as e:
            log.error(f"Error writing batch {batch_id} to database: {str(e)}")
    else:
        log.warning(f"Batch {batch_id} had no records to write.")

# === Spark Stream Startup ===
def start_stream():
    spark = SparkSession.builder \
        .master("spark://localhost:7077") \
        .appName("HeartRateStreamProcessor") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config(
            "spark.jars",
            "postgresql-42.7.5.jar"
        ) \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
        ) \
        .getOrCreate()

    log.info("Spark session started")

    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

    db_query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_FOLDER) \
        .start()

    # Optional console output
    df_parsed.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    log.info("Streaming query started")
    db_query.awaitTermination()

# === Kafka Producer Thread ===
def background_data_producer():
    send_to_kafka = create_kafka_producer(kafka_broker)
    while True:
        records = stream_heart_rate_data(num_athletes=10, num_records=10)
        process_and_stream_data(records, send_to_kafka, KAFKA_TOPIC)
        time.sleep(5)

# === Entry Point ===
if __name__ == "__main__":
    log.info("Starting Heart Rate Streaming Pipeline")

    # Start background Kafka producer thread
    producer_thread = threading.Thread(target=background_data_producer, daemon=True)
    producer_thread.start()

    # Start Spark stream
    start_stream()
