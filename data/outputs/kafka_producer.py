import json
import time
import uuid
from datetime import datetime
import os
import sys

# Path Logic
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.abspath(os.path.join(current_dir, ".."))
project_root = os.path.abspath(os.path.join(data_dir, ".."))
for p in [project_root, data_dir]:
    if p not in sys.path:
        sys.path.append(p)

try:
    from kafka import KafkaProducer
    from minio import Minio
except ImportError:
    import subprocess

    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "kafka-python", "minio", "-q"]
    )
    from kafka import KafkaProducer
    from minio import Minio

from utils.audit_writer import AuditWriter

try:
    from config import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPICS,
        KAFKA_SEND_DELAY,
        MINIO_CONFIG,
        MINIO_BUCKET,
    )
except ImportError:
    try:
        from data.config import (
            KAFKA_BOOTSTRAP_SERVERS,
            KAFKA_TOPICS,
            KAFKA_SEND_DELAY,
            MINIO_CONFIG,
            MINIO_BUCKET,
        )
    except ImportError:
        KAFKA_BOOTSTRAP_SERVERS = "kafka1:19092"
        KAFKA_TOPICS = {}
        KAFKA_SEND_DELAY = 1.0
        MINIO_CONFIG = {}
        MINIO_BUCKET = "landing-zone"


class SmartCityKafkaProducer:
    def __init__(self):
        self.producer = None
        self.minio_client = None
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.minio_config = MINIO_CONFIG
        self.minio_bucket = MINIO_BUCKET
        self.audit = None

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"Kafka Producer connected to {self.bootstrap_servers}")
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            raise

        try:
            self.minio_client = Minio(**self.minio_config)
            if not self.minio_client.bucket_exists(self.minio_bucket):
                self.minio_client.make_bucket(self.minio_bucket)
            print(f"MinIO Client connected to {self.minio_config['endpoint']}")
            self.audit = AuditWriter(self.minio_client, self.minio_bucket)
        except Exception as e:
            print(f"MinIO connection failed: {e}")
            raise

    def write(self, domain, records):
        if self.producer is None or self.minio_client is None:
            self._connect()

        topic = KAFKA_TOPICS.get(domain)
        if not topic:
            print(f"No Kafka topic configured for domain: {domain}")
            return

        print(f"Streaming individual records to Kafka and MinIO ({topic})...")

        count = 0
        now = datetime.now()
        for record in records:
            try:
                self.producer.send(topic, record)
                if self.audit:
                    try:
                        self.audit.write(
                            pipeline_type="Stream",
                            source="streaming_producer",
                            batch_id=f"event_{record.get('id', str(uuid.uuid4())[:8])}",
                            start_time=now,
                            end_time=datetime.now(),
                            record_count=1,
                            records=[record],
                            status="SUCCESS",
                        )
                    except Exception:
                        pass

                count += 1
                if KAFKA_SEND_DELAY > 0:
                    time.sleep(KAFKA_SEND_DELAY)

            except Exception as e:
                print(f"Failed to process record: {e}")

        self.producer.flush()
        print(f"Successfully sent {count} records to Kafka.")

    def close(self):
        if self.producer:
            self.producer.close()
