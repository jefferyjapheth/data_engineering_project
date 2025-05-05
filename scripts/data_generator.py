#!/usr/bin/env python3
"""
Streaming-Friendly Sports Heart Rate Generator
Generates synthetic heart rate data for athletes in a sports event.
"""
import random
import logging
from datetime import datetime, timedelta
from time import sleep

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("sports_hr_streamer")

def generate_athlete_ids(num_athletes):
    """Generates realistic athlete IDs like ATH001, ATH002, etc."""
    return [f"ATH{str(i).zfill(3)}" for i in range(1, num_athletes + 1)]

def generate_heart_rate_record(athlete_id, event_start_time):
    """Generates a single heart rate record for an athlete."""
    time_offset = random.randint(0, 2 * 60 * 60)  # Picks a random time within the last 2 hours (2 * 60 * 60 = 7200 seconds)
    timestamp = event_start_time + timedelta(seconds=time_offset)
    heart_rate = random.randint(90, 180) # Simulating heart rate between 90 and 180 bpm
    return {
        "athlete_id": athlete_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": heart_rate
    } # Example: {'athlete_id': 'ATH001', 'timestamp': '2023-10-01 12:00:00', 'heart_rate': 120}

def stream_heart_rate_data(num_athletes=10, num_records=100, delay=0.1):
    """
    Streams synthetic heart rate data.

    Args:
        num_athletes: Total number of athletes
        num_records: Number of records to stream
        delay: Time delay (in seconds) between records (simulates real streaming)

    Yields:
        dict: A heart rate data record
    """
    athlete_ids = generate_athlete_ids(num_athletes) # Generate athlete IDs based on the number of athletes
    start_time = datetime.now() - timedelta(hours=2)

    for _ in range(num_records):
        athlete_id = random.choice(athlete_ids)
        record = generate_heart_rate_record(athlete_id, start_time)
        yield record
        sleep(delay)  # Simulate streaming delay

if __name__ == "__main__":
    try:
        for record in stream_heart_rate_data(num_athletes=10, num_records=50, delay=0.2):
            logger.info(record)  # Replace with send_to_kafka(record) in real use
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
