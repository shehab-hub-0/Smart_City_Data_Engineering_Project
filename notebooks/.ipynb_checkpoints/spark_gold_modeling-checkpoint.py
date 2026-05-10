import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# --- 1. Smart Configuration ---
IS_DOCKER = os.path.exists('/.dockerenv')

if IS_DOCKER:
    MINIO_ENDPOINT = "http://minio:9000"
    NESSIE_URL = "http://nessie:19120/api/v1"
else:
    MINIO_ENDPOINT = "http://localhost:9005"
    NESSIE_URL = "http://localhost:19120/api/v1"

SILVER_TABLE = "nessie.silver_city_events"
FACT_TABLE = "nessie.gold_fact_city_events"
DIM_ZONE = "nessie.gold_dim_zone"

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

def main():
    spark = SparkSession.builder \
        .appName("Gold_Modeling") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.nessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.uri", NESSIE_URL) \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    print(f"📖 Reading from {SILVER_TABLE}...")
    silver_df = spark.table(SILVER_TABLE)

    # 1. Create Fact Table
    fact_df = silver_df.select(
        "id", "timestamp", "zone", "latitude", "longitude",
        "pm2_5_val", "vehicle_count_val", "weather_temperature", "_ingestion_time"
    ).withColumn("_gold_modeling_time", current_timestamp())

    print(f"🚀 Writing Fact Table to {FACT_TABLE}...")
    fact_df.writeTo(FACT_TABLE).createOrReplace()

    # 2. Create Dimension Table (Zones)
    dim_zone_df = silver_df.select("zone").distinct()
    print(f"🚀 Writing Dimension Table to {DIM_ZONE}...")
    dim_zone_df.writeTo(DIM_ZONE).createOrReplace()

    print("✅ Gold Modeling Complete.")

if __name__ == "__main__":
    main()
