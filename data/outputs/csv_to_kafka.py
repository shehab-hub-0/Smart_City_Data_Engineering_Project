import os
import pandas as pd
import json
from datetime import datetime
from kafka import KafkaProducer

try:
    from config import CSV_OUTPUT_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
except ImportError:
    from data.config import CSV_OUTPUT_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS


class CSVToKafka:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    def ingest_daily_csv(self, target_date=None):
        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")

        print(f"📂 Scanning for CSV files for date: {target_date}")

        for domain, topic in KAFKA_TOPICS.items():
            domain_path = os.path.join(CSV_OUTPUT_DIR, domain, f"date={target_date}")

            if not os.path.exists(domain_path):
                print(f"⚠️ No directory found for {domain} on {target_date}")
                continue

            csv_files = [f for f in os.listdir(domain_path) if f.endswith(".csv")]

            for csv_file in csv_files:
                file_path = os.path.join(domain_path, csv_file)
                print(f"📖 Reading {file_path}...")

                df = pd.read_csv(file_path)
                records = df.to_dict(orient="records")

                print(f"📤 Sending {len(records)} records to {topic}...")
                for record in records:
                    record["metadata_source_type"] = "csv"
                    self.producer.send(topic, record)

        self.producer.flush()
        print("✅ CSV Ingestion to Kafka completed.")


if __name__ == "__main__":
    ingestor = CSVToKafka()
    ingestor.ingest_daily_csv()
