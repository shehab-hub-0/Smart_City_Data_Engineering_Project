import psycopg2
from psycopg2.extras import RealDictCursor
import json
from kafka import KafkaProducer

try:
    from config import DB_CONFIG, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
except ImportError:
    from data.config import DB_CONFIG, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS


class DBToKafka:
    def __init__(self):
        self.db_config = DB_CONFIG
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def ingest_db_to_kafka(self):
        print("🐘 Connecting to PostgreSQL for ingestion...")
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor(cursor_factory=RealDictCursor)

            for domain, topic in KAFKA_TOPICS.items():
                if domain == "unified":
                    continue

                table_name = f"raw_{domain}"
                print(f"🔍 Reading from table: {table_name}")

                try:
                    cur.execute(f"SELECT * FROM public.{table_name};")
                    records = cur.fetchall()

                    if not records:
                        print(f"ℹ️ No records found in {table_name}")
                        continue

                    print(f"📤 Sending {len(records)} records from DB to {topic}...")
                    for record in records:
                        record_dict = dict(record)
                        record_dict["metadata_source_type"] = "database"
                        self.producer.send(topic, record_dict)

                except Exception as e:
                    print(f"❌ Error reading table {table_name}: {e}")
                    conn.rollback()
                    continue

            self.producer.flush()
            cur.close()
            conn.close()
            print("✅ Database Ingestion to Kafka completed.")

        except Exception as e:
            print(f"❌ DB connection failed: {e}")


if __name__ == "__main__":
    ingestor = DBToKafka()
    ingestor.ingest_db_to_kafka()
