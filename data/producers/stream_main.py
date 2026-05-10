import time
import json
import socket
import logging
import random
from datetime import datetime
from confluent_kafka import Producer

# Import the Unified Generator from our data system
# This ensures Stream and Batch use the SAME logic and SAME noise patterns
try:
    from generator.unified_generator import UnifiedGenerator
except ImportError:
    import sys
    import os

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from generator.unified_generator import UnifiedGenerator

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "smartcity.stream"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("StreamProducer")

conf = {"bootstrap.servers": KAFKA_BROKER, "client.id": socket.gethostname()}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        pass  # Success


def run_streaming():
    logger.info("🚀 Starting Unified Streaming Producer (Consistent with Batch)...")
    gen = UnifiedGenerator()

    try:
        while True:
            # Generate a record using the same logic as Batch
            record = gen.generate_record(base_time=datetime.now())

            # Override metadata to indicate it's streaming
            record["metadata_source_type"] = "stream"

            # Serialize to JSON
            payload = json.dumps(record, default=str)

            # Produce to Kafka
            producer.produce(KAFKA_TOPIC, value=payload, callback=delivery_report)

            # Poll for delivery reports
            producer.poll(0)

            # Small delay to simulate real-world sensor frequency
            time.sleep(random.uniform(0.5, 2.0))

            if random.random() < 0.01:  # Log every ~100 records
                logger.info(
                    f"Last produced record: {record['id']} in zone {record['zone']}"
                )

    except KeyboardInterrupt:
        logger.info("🛑 Streaming stopped by user.")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_streaming()
