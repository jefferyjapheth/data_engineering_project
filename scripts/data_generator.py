import os
import time
import random
from datetime import datetime, timedelta
from time import sleep

from kafka_producer import create_kafka_producer  # custom Kafka producer function
from utils.logger import setup_logger  # custom logger setup

# === Configuration ===
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sports_athlete_heartrates")  # Kafka topic name

# === Logger Setup ===
logger = setup_logger("sports_hr_streamer")

# === Helper Functions ===

def generate_athlete_ids(num_athletes):
    """
    Generates realistic athlete IDs like ATH001, ATH002, etc.
    """
    return [f"ATH{str(i).zfill(3)}" for i in range(1, num_athletes + 1)]

def generate_activity_status():
    """
    Randomly pick an activity status: resting, warming_up, or active.
    """
    return random.choice(["resting", "warming_up", "active"])

def generate_heart_rate_record(athlete_id, event_start_time):
    """
    Generates a simulated heart rate record for an athlete.

    - Simulates a timestamp offset up to 60 minutes from the base time.
    - Adjusts heart rate based on activity status.
    """
    activity_status = generate_activity_status()
    heart_rate = random.randint(20, 210) if activity_status == "active" else random.randint(60, 120)

    # Randomize timestamp within the last hour
    time_offset = random.randint(0, 60 * 60)
    timestamp = event_start_time + timedelta(seconds=time_offset)

    return {
        "athlete_id": athlete_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": heart_rate,
        "activity_status": activity_status
    }

def stream_heart_rate_data(num_athletes=10, num_records=100, delay=0.1):
    """
    Generates a batch of simulated heart rate data for multiple athletes.
    """
    athlete_ids = generate_athlete_ids(num_athletes)
    start_time = datetime.now() - timedelta(hours=1)  # Base time for generating timestamps

    # Generate a list of simulated heart rate records
    records = [
        generate_heart_rate_record(random.choice(athlete_ids), start_time)
        for _ in range(num_records)
    ]
    return records

def process_and_stream_data(records, send_record, topic, delay=0.1):
    """
    Sends records to Kafka with a small delay between each send.
    """
    for record in records:
        send_record(record, topic)
        sleep(delay)

# === Main Entry Point ===
if __name__ == "__main__":
    kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')  # Kafka broker URL

    # Create Kafka producer (returns a send_record function)
    send_to_kafka = create_kafka_producer(kafka_broker)

    # Continuously generate and stream heart rate data to Kafka
    while True:
        records = stream_heart_rate_data(num_athletes=10, num_records=10)
        process_and_stream_data(records, send_to_kafka, KAFKA_TOPIC)
        time.sleep(5)  # Wait before producing the next batch
