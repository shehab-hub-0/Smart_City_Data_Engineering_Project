from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka.admin import KafkaAdminClient, NewTopic
from datetime import datetime, timedelta
import logging
import time

# Configuration (imported from settings if possible, but hardcoded here for DAG isolation)
KAFKA_BROKER = "kafka1:19092"
TOPICS = ["smartcity.stream", "smartcity.batch"]

default_args = {
    "owner": "antigravity",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def cleanup_kafka_logic():
    logging.info(f"📡 Connecting to Kafka Admin at {KAFKA_BROKER}...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER, client_id="airflow_reset_dag"
    )

    # 1. Delete Topics
    logging.info(f"🗑️ Attempting to delete topics: {TOPICS}")
    try:
        admin_client.delete_topics(topics=TOPICS)
        logging.info("✅ Deletion request sent.")
    except Exception as e:
        logging.warning(f"ℹ️ Some topics might not exist: {e}")

    logging.info("⏳ Waiting 10 seconds for Kafka to synchronize...")
    time.sleep(10)

    # 2. Recreate Topics
    logging.info(f"🆕 Recreating topics: {TOPICS}")
    new_topics = [
        NewTopic(name=t, num_partitions=1, replication_factor=1) for t in TOPICS
    ]
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        logging.info("✅ Topics recreated successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to recreate topics: {e}")
    finally:
        admin_client.close()


def purge_minio_logic():
    import boto3
    from botocore.client import Config

    logger = logging.getLogger("MinIOPurge")

    s3 = boto3.resource(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    buckets = ["warehouse", "spark-checkpoints"]

    for bucket_name in buckets:
        logger.info(f"🧹 Purging bucket: {bucket_name}")
        try:
            bucket = s3.Bucket(bucket_name)
            # Delete all objects
            bucket.objects.all().delete()
            logger.info(f"✅ Bucket {bucket_name} cleared.")
        except Exception as e:
            logger.warning(
                f"⚠️ Failed to purge bucket {bucket_name} (might not exist): {e}"
            )


def purge_nessie_catalog():
    import requests

    # Using API v1 as it is often more stable for simple commits
    NESSIE_API = "http://nessie:19120/api/v1"

    logger = logging.getLogger("NessiePurge")

    logger.info("🔍 Fetching all entries from Nessie 'main' branch (v1)...")
    try:
        # 1. Get entries
        resp = requests.get(f"{NESSIE_API}/trees/tree/main/entries")
        resp.raise_for_status()
        entries = resp.json().get("entries", [])

        if not entries:
            logger.info("✨ Nessie catalog is already empty.")
            return

        # 2. Get current hash
        tree_resp = requests.get(f"{NESSIE_API}/trees/tree/main")
        tree_resp.raise_for_status()
        current_hash = tree_resp.json().get("hash")

        ops = []
        for entry in entries:
            key_elements = entry.get("name", {}).get("elements", [])
            logger.info(f"🗑️ Queueing deletion for: {'.'.join(key_elements)}")
            ops.append({"type": "DELETE", "key": {"elements": key_elements}})

        commit_payload = {
            "commitMeta": {"message": "Full catalog reset via Airflow DAG (v1 API)"},
            "operations": ops,
        }

        commit_resp = requests.post(
            f"{NESSIE_API}/trees/branch/main/commit?expectedHash={current_hash}",
            json=commit_payload,
        )
        if commit_resp.status_code != 204 and commit_resp.status_code != 200:
            logger.error(
                f"❌ Commit failed: {commit_resp.status_code} - {commit_resp.text}"
            )
            commit_resp.raise_for_status()

        logger.info("✅ Nessie catalog purged successfully via v1 API.")

    except Exception as e:
        logger.error(f"❌ Nessie purge failed: {e}")


with DAG(
    "medallion_reset_pipeline",
    default_args=default_args,
    description="Full Reset of Kafka, Iceberg Tables, and MinIO Checkpoints",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["maintenance", "medallion"],
) as dag:

    kafka_cleanup = PythonOperator(
        task_id="cleanup_kafka_topics",
        python_callable=cleanup_kafka_logic,
    )

    minio_purge = PythonOperator(
        task_id="purge_minio_and_checkpoints",
        python_callable=purge_minio_logic,
    )

    nessie_purge = PythonOperator(
        task_id="purge_nessie_catalog",
        python_callable=purge_nessie_catalog,
    )

    # Execution Flow: Kafka -> MinIO -> Nessie
    kafka_cleanup >> minio_purge >> nessie_purge
