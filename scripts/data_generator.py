import os
import time
import random
from datetime import datetime, timedelta

from kafka_producer import create_kafka_producer  # optimized Kafka producer
from logger import setup_logger  # custom logger

# === Configuration ===
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sports_athlete_heartrates")  # Kafka topic
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092") # Kafka broker
BATCH_SIZE = int(os.getenv("HR_BATCH_SIZE", 200))  # Number of records per batch
BATCH_INTERVAL = float(os.getenv("HR_BATCH_INTERVAL", 5))  # Seconds between batches
NUM_ATHLETES = int(os.getenv("NUM_ATHLETES", 50))  # Number of simulated athletes

# === Logger Setup ===
logger = setup_logger("sports_hr_streamer")

# === Helper Functions ===

def generate_athlete_ids(num_athletes):
    return [f"ATH{str(i).zfill(3)}" for i in range(1, num_athletes + 1)]

def generate_activity_status():
    return random.choice(["resting", "warming_up", "active"])

def generate_heart_rate_record(athlete_id, event_start_time):
    activity_status = generate_activity_status()
    heart_rate = random.randint(20, 210) if activity_status == "active" else random.randint(60, 120)
    time_offset = random.randint(0, 60 * 60)
    timestamp = event_start_time + timedelta(seconds=time_offset)
    return {
        "athlete_id": athlete_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": heart_rate,
        "activity_status": activity_status
    }

def stream_heart_rate_data(num_athletes, num_records):
    """
    Generate a list of simulated heart rate records.
    """
    athlete_ids = generate_athlete_ids(num_athletes)
    start_time = datetime.now() - timedelta(hours=1)
    return [
        generate_heart_rate_record(random.choice(athlete_ids), start_time)
        for _ in range(num_records)
    ]

def process_and_stream_batch(records, send_record, topic):
    """
    Send a batch of records to Kafka.
    """
    for record in records:
        send_record(record, topic)
    logger.info(f"Sent batch of {len(records)} heart rate records to topic '{topic}'")

# === Main Entry Point ===
if __name__ == "__main__":
    send_to_kafka = create_kafka_producer(KAFKA_BROKER)

    while True:
        # Generate batch of heart rate records
        records = stream_heart_rate_data(num_athletes=NUM_ATHLETES, num_records=BATCH_SIZE)

        # Send batch to Kafka
        process_and_stream_batch(records, send_to_kafka, KAFKA_TOPIC)

        # Wait between batches (microbatch interval)
        time.sleep(BATCH_INTERVAL)