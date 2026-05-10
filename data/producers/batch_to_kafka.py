import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer
import socket

try:
    from config import DB_CONFIG, CSV_OUTPUT_DIR
except ImportError:
    from producers.config import DB_CONFIG, CSV_OUTPUT_DIR

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka1:19092")
KAFKA_TOPIC = "smartcity.batch"

# Path for CSVs
CSV_DIR = CSV_OUTPUT_DIR

conf = {"bootstrap.servers": KAFKA_BROKER, "client.id": socket.gethostname()}
producer = Producer(conf)


def send_to_kafka(data_dict):
    data_dict["metadata_source_type"] = "batch"
    data_dict["_source_format"] = "batch_unified"  # Adding the missing column
    producer.produce(
        KAFKA_TOPIC, value=json.dumps(data_dict, default=str).encode("utf-8")
    )


def process_csv_files():
    print(f"📂 Checking for CSV files in {CSV_DIR}...")
    if not os.path.exists(CSV_DIR):
        return 0

    count = 0
    # Walk bottom-up to allow deleting subdirectories after files
    for root, _dirs, files in os.walk(CSV_DIR, topdown=False):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(file_path)
                    for rec in df.to_dict(orient="records"):
                        send_to_kafka(rec)
                        count += 1
                    # --- CLEANUP: Delete file after processing ---
                    os.remove(file_path)
                except Exception as e:
                    print(f"❌ Error CSV: {e}")

        # --- CLEANUP: Delete empty subdirectories (date folders) ---
        if root != CSV_DIR:  # Don't delete the root CSV_DIR itself
            try:
                if not os.listdir(root):  # If directory is empty
                    os.rmdir(root)
                    print(f"🧹 Removed empty folder: {os.path.basename(root)}")
            except Exception:
                pass
    return count


def process_db_data():
    print(f"🐘 Checking for data in Postgres Table: raw_unified...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT * FROM public.raw_unified;")
        rows = cur.fetchall()

        for row in rows:
            send_to_kafka(dict(row))

        # --- CLEANUP: Truncate table after processing ---
        if len(rows) > 0:
            cur.execute("TRUNCATE TABLE public.raw_unified;")
            conn.commit()
            print(f"🧹 Table raw_unified truncated.")

        cur.close()
        conn.close()
        return len(rows)
    except Exception as e:
        print(f"❌ Error DB: {e}")
        return 0


def main():
    print(f"🚀 Batch Ingestion Pipeline Started (with Cleanup)...")

    csv_count = process_csv_files()
    db_count = process_db_data()

    producer.flush()
    print(
        f"🏁 Summary: Sent {csv_count} CSV records and {db_count} DB records to Kafka."
    )
    print("✨ Environment cleaned for the next run.")


if __name__ == "__main__":
    main()
