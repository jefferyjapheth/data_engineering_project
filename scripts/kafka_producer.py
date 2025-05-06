import sys
import os
import json

# Add project root to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaProducer
from utils.logger import setup_logger

# Setup logging
logger = setup_logger("kafka_producer")

def create_kafka_producer(kafka_broker):
    """
    Returns a Kafka producer function that can send heart rate records.
    
    Args:
        kafka_broker (str): Kafka broker address (e.g., 'localhost:9092')
    
    Returns:
        function: A function to send records to Kafka
    """
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    def send_record(record, topic):
        """Sends a heart rate record to Kafka."""
        try:
            producer.send(topic, value=record)
            producer.flush()
            logger.info(f"Sent record: {record}")
        except Exception as e:
            logger.error(f"Failed to send record: {e}")

    return send_record
