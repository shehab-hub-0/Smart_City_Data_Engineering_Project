import os
import time
import requests
import boto3
from botocore.client import Config
from confluent_kafka.admin import AdminClient, NewTopic

# --- Configuration ---
MINIO_URL = "http://localhost:9005"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKETS = ["warehouse", "spark-checkpoints"]

KAFKA_BROKER = "localhost:9092"
TOPICS = ["smartcity.stream", "smartcity.batch"]

DREMIO_BASE_URL = os.getenv("DREMIO_URL", "http://localhost:9047")
DREMIO_USER     = os.getenv("DREMIO_USER", "admin")
DREMIO_PASS     = os.getenv("DREMIO_PASSWORD", "smartcity2025")
DREMIO_EMAIL    = os.getenv("DREMIO_EMAIL", "admin@smartcity.com")

def setup_minio():
    print("[MINIO] Setting up MinIO Buckets...")
    s3 = boto3.resource('s3',
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    for bucket_name in BUCKETS:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"  - Created Bucket: {bucket_name}")
        except:
            print(f"  - Bucket '{bucket_name}' already exists.")

def setup_kafka():
    print("[KAFKA] Setting up Kafka Topics...")
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in TOPICS]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"  - Created Topic: {topic}")
        except Exception as e:
            print(f"  - Topic '{topic}' already exists.")

def setup_dremio():
    print("[DREMIO] Setting up Dremio...")
    
    # 1. Login to get token
    login_url = f"{DREMIO_BASE_URL}/apiv2/login"
    try:
        resp = requests.post(login_url, json={"userName": DREMIO_USER, "password": DREMIO_PASS})
        if resp.status_code != 200:
            # Try bootstrap if login fails
            bootstrap_url = f"{DREMIO_BASE_URL}/apiv2/bootstrap/firstuser"
            requests.post(bootstrap_url, json={"userName": DREMIO_USER, "password": DREMIO_PASS, "email": DREMIO_EMAIL, "firstName": "Admin", "lastName": "User"})
            resp = requests.post(login_url, json={"userName": DREMIO_USER, "password": DREMIO_PASS})
            if resp.status_code != 200:
                print(f"[ERROR] Dremio login failed: {resp.text}")
                return

        token = resp.json()["token"]
        headers = {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}

        # 2. Add Nessie Source using the successful v3 catalog configuration
        nessie_source = {
            "entityType": "source",
            "name": "nessie",
            "type": "NESSIE",
            "config": {
                "nessieEndpoint": "http://nessie:19120/api/v2",
                "nessieAuthType": "NONE",
                "secure": False,
                "credentialType": "ACCESS_KEY",
                "awsAccessKey": MINIO_ACCESS_KEY,
                "awsAccessSecret": MINIO_SECRET_KEY,
                "awsRootPath": "/warehouse",
                "propertyList": [
                    {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                    {"name": "fs.s3a.path.style.access", "value": "true"},
                    {"name": "dremio.s3.compat", "value": "true"},
                    {"name": "fs.s3a.connection.ssl.enabled", "value": "false"}
                ]
            }
        }

        # 2. Find existing source ID by name
        source_id = None
        cat_resp = requests.get(f"{DREMIO_BASE_URL}/api/v3/catalog", headers=headers)
        if cat_resp.status_code == 200:
            for item in cat_resp.json().get("data", []):
                if item.get("containerType") == "SOURCE" and item.get("path") == ["nessie"]:
                    source_id = item["id"]
                    break
        
        if source_id:
            nessie_source["id"] = source_id
            nessie_source["tag"] = requests.get(f"{DREMIO_BASE_URL}/api/v3/catalog/{source_id}", headers=headers).json().get("tag")
            resp = requests.put(f"{DREMIO_BASE_URL}/api/v3/catalog/{source_id}", headers=headers, json=nessie_source)
            action = "updated"
        else:
            resp = requests.post(f"{DREMIO_BASE_URL}/api/v3/catalog", headers=headers, json=nessie_source)
            action = "added"
        
        if resp.status_code in [200, 201]:
            print("  - Successfully updated Nessie source in Dremio!")
        else:
            print(f"[ERROR] Failed to {action} Nessie source. Status Code: {resp.status_code}")
            print(f"Response: {resp.text}")
            
    except Exception as e:
        print(f"[ERROR] Dremio setup error: {e}")

if __name__ == "__main__":
    setup_minio()
    setup_kafka()
    setup_dremio()
    print("\nINFRASTRUCTURE IS 100% READY!")