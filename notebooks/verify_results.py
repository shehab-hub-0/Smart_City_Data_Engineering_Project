from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("VerifyGoldKPIs") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkExtensions") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("--- GOLD HOURLY ZONE METRICS (New KPIs) ---")
spark.sql("""
    SELECT zone, window_start, event_count, unique_sensor_count, 
           infrastructure_load, safety_score, data_reliability_pct 
    FROM nessie.gold_hourly_zone_metrics 
    ORDER BY window_start DESC 
    LIMIT 10
""").show()

print("--- SILVER DIMENSIONS CHECK ---")
print("Locations Count:", spark.sql("SELECT count(*) FROM nessie.silver_dim_locations").collect()[0][0])
print("Sensors Count:", spark.sql("SELECT count(*) FROM nessie.silver_dim_sensors").collect()[0][0])

spark.stop()
