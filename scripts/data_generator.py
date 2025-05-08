
import os
import time
import random
from datetime import datetime, timedelta
from time import sleep
from kafka_producer import create_kafka_producer
from utils.logger import setup_logger

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sports_athlete_heartrates")

# Setup logger
logger = setup_logger("sports_hr_streamer")

def generate_athlete_ids(num_athletes):
    """Generates realistic athlete IDs like ATH001, ATH002, etc."""
    return [f"ATH{str(i).zfill(3)}" for i in range(1, num_athletes + 1)]

def generate_activity_status():
    return random.choice(["resting", "warming_up", "active"])

def generate_heart_rate_record(athlete_id, event_start_time):
    """Generates a single heart rate record for an athlete based on their activity."""
    activity_status = generate_activity_status()
    heart_rate = random.randint(60, 180) if activity_status == "active" else random.randint(60, 120)

    # Simulate events within the last 30 minutes to 1 hour
    time_offset = random.randint(0, 60 * 60)
    timestamp = event_start_time + timedelta(seconds=time_offset)

    return {
        "athlete_id": athlete_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": heart_rate,
        "activity_status": activity_status
    }

def stream_heart_rate_data(num_athletes=10, num_records=100, delay=0.1):
    """Generate heart rate data records within a shorter time window."""
    athlete_ids = generate_athlete_ids(num_athletes)
    start_time = datetime.now() - timedelta(hours=1)
    records = [
        generate_heart_rate_record(random.choice(athlete_ids), start_time)
        for _ in range(num_records)
    ]
    return records

def process_and_stream_data(records, send_record, topic, delay=0.1):
    """Streams heart rate data to Kafka."""
    for record in records:
        send_record(record, topic)
        sleep(delay)

if __name__ == "__main__":
    # Kafka configuration
    kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')  # Kafka broker URL
    #topic = "sports_heart_rate"

    # Create Kafka producer
    send_to_kafka = create_kafka_producer(kafka_broker)
    while True:
        records = stream_heart_rate_data(num_athletes=10, num_records=10)
        process_and_stream_data(records, send_to_kafka, KAFKA_TOPIC)
        time.sleep(5)