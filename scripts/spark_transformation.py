import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, last
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from logger import setup_logger

# Set up logging
log = setup_logger("stream_processor")

# === Load Environment Variables ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sports_athlete_heartrates")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://db:5432/heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Amalitech")
POSTGRES_DRIVER = os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver")
TABLE_NAME = os.getenv("TABLE_NAME", "ath_heartbeats")
CHECKPOINT_FOLDER = os.getenv("CHECKPOINT_FOLDER", "/tmp/spark_checkpoint")
RUNNING_IN_DOCKER = os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"

# === Generate a Unique Checkpoint Folder ===
# Append current timestamp to make sure the checkpoint folder is unique
timestamp = time.strftime("%Y%m%d%H%M%S")
CHECKPOINT_FOLDER_UNIQUE = f"{CHECKPOINT_FOLDER}_{timestamp}"

log.info(f"Using checkpoint directory: {CHECKPOINT_FOLDER_UNIQUE}")

# Determine correct Kafka broker
kafka_broker = KAFKA_BROKER 

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
        try:
            # Convert timestamp to actual timestamp type
            df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

            # Replace 0 with null for heart_rate
            df = df.withColumn("heart_rate", when(col("heart_rate") == 0, None).otherwise(col("heart_rate")))

            # Define a window to carry forward last valid heart_rate per athlete
            window_spec = Window.partitionBy("athlete_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)

            # Fill heart_rate with last non-null value
            df = df.withColumn("heart_rate_filled", last("heart_rate", ignorenulls=True).over(window_spec))

            # Drop original and rename
            df = df.drop("heart_rate").withColumnRenamed("heart_rate_filled", "heart_rate")

            # Write to DB
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
        except Exception as e:
            log.error(f"Error writing batch {batch_id} to database: {str(e)}")
    else:
        log.warning(f"Batch {batch_id} had no records to write.")

# === Spark Stream Startup ===
def start_stream():
    # Create Spark session with optimizations
    spark = SparkSession.builder \
        .appName("HeartRateStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.streaming.kafka.maxOffsetsPerTrigger", 1000) \
        .config("spark.streaming.kafka.maxRatePerPartition", 500) \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "10s") \
        .getOrCreate()  # Creates or retrieves an existing Spark session

    log.info("Spark session started")  # Logs the start of the Spark session

    # Notes:
    # - shuffle.partitions (Adjust shuffle partitions for optimal performance)
    # - maxOffsetsPerTrigger (Limit the offsets processed per trigger)
    # - maxRatePerPartition (Limit max rate per partition to prevent overloading)
    # - stateStore.maintenanceInterval (State store cleanup interval)


    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), json_schema).alias("data")) \
        .select("data.*")

    db_query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_FOLDER_UNIQUE) \
        .start()

    db_query.awaitTermination()

# === Entry Point ===
if __name__ == "__main__":
    log.info("Starting Heart Rate Streaming Pipeline")
    start_stream()
