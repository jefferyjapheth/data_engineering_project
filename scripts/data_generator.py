import random
from datetime import datetime, timedelta
from time import sleep
from kafka_producer import create_kafka_producer
from utils.logger import setup_logger

# Setup logger
logger = setup_logger("sports_hr_streamer")

def generate_athlete_ids(num_athletes):
    """Generates realistic athlete IDs like ATH001, ATH002, etc."""
    return [f"ATH{str(i).zfill(3)}" for i in range(1, num_athletes + 1)]

def generate_heart_rate_record(athlete_id, event_start_time):
    """Generates a single heart rate record for an athlete."""
    time_offset = random.randint(0, 2 * 60 * 60)  # Picks a random time within the last 2 hours
    timestamp = event_start_time + timedelta(seconds=time_offset)
    heart_rate = random.randint(90, 180)  # Simulating heart rate between 90 and 180 bpm
    return {
        "athlete_id": athlete_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": heart_rate
    }

def stream_heart_rate_data(num_athletes=10, num_records=100, delay=0.1):
    """
    Generates and returns heart rate data records.

    Args:
        num_athletes: Total number of athletes
        num_records: Number of records to generate
        delay: Time delay between records (simulates streaming)
    
    Returns:
        list: A list of heart rate data records
    """
    athlete_ids = generate_athlete_ids(num_athletes)
    start_time = datetime.now() - timedelta(hours=2)
    records = [
        generate_heart_rate_record(random.choice(athlete_ids), start_time) 
        for _ in range(num_records)
    ]
    return records

def process_and_stream_data(records, send_record, topic, delay=0.1):
    """Streams heart rate data to Kafka."""
    for record in records:
        send_record(record, topic)
        sleep(delay)  # Simulate streaming delay

if __name__ == "__main__":
    # Kafka configuration
    kafka_broker = "localhost:9092"  # Adjust to your Kafka broker address
    topic = "sports_heart_rate"

    # Create Kafka producer
    send_to_kafka = create_kafka_producer(kafka_broker)

    # Generate heart rate data
    records = stream_heart_rate_data(num_athletes=10, num_records=50, delay=0.2)

    # Process and stream data to Kafka
    try:
        process_and_stream_data(records, send_to_kafka, topic)
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
