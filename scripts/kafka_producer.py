
import json
import time
from kafka import KafkaProducer
from logger import setup_logger



# Setup logging
logger = setup_logger("kafka_producer")

def create_kafka_producer(kafka_broker):
    """
    Returns a Kafka producer function that can send heart rate records.
    
    Optimizations:
    - Microbatching: Enabled by configuring `linger_ms` and `batch_size`.
    - Reliability: Configured retries and acknowledgment strategy.
    - Latency & failure handling: Custom timeouts and retry logic with backoff.
    
    Args:
        kafka_broker (str): Kafka broker address (e.g., 'localhost:9092')
    
    Returns:
        function: A function to send records to Kafka
    """
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),

        # Wait up to 10ms before sending a batch to increase batching opportunity
        linger_ms=10,

        # Maximum batch size (in bytes); larger batches improve throughput
        batch_size=32 * 1024,  # 32 KB

        # Wait for all replicas to acknowledge before considering send successful
        acks='all',

        # Retry up to 5 times on transient failures (e.g., network hiccups)
        retries=5,

        # Time to wait for a broker response before considering the request failed
        request_timeout_ms=15000,  # 15 seconds

        # Time a `send()` call can block if buffer is full
        max_block_ms=10000  # 10 seconds
    )

    def send_record(record, topic, retries=3, backoff_sec=2):
        """
        Sends a heart rate record to Kafka with retry and backoff logic.

        Args:
            record (dict): The data to send.
            topic (str): Kafka topic.
            retries (int): Number of retry attempts on failure.
            backoff_sec (int): Delay between retries.
        """
        for attempt in range(1, retries + 1):
            try:
                producer.send(topic, value=record)
                logger.info(f"Sent record on attempt {attempt}: {record}")
                break  # Break on success
            except Exception as e:
                logger.error(f"Attempt {attempt} failed: {e}")
                time.sleep(backoff_sec)
        else:
            logger.error(f"All {retries} attempts failed for record: {record}")

    return send_record