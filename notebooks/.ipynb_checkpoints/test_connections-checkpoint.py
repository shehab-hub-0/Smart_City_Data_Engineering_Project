import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    print("🚀 Starting integration test for Spark -> MinIO -> PostgreSQL")
    
    # Initialize Spark with necessary packages for MinIO (S3) and PostgreSQL JDBC
    spark = SparkSession.builder \
        .appName("Test-E2E-Integration") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Create a dummy dataframe
    print("📊 1. Creating Dummy DataFrame...")
    data = [
        (1, "Sensor-A", 450),
        (2, "Sensor-B", 601),
        (3, "Sensor-C", 805)
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("sensor_name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    df.show()
    
    # 2. Write to MinIO
    minio_path = "s3a://raw-data/test_spark_integration.parquet"
    print(f"💾 2. Writing to MinIO at {minio_path}...")
    try:
        df.write.mode("overwrite").parquet(minio_path)
        print("✅ Successfully wrote to MinIO.")
    except Exception as e:
        print(f"❌ Failed to write to MinIO: {e}")
        spark.stop()
        raise e
    
    # 3. Read from MinIO
    print(f"📂 3. Reading back from MinIO ({minio_path})...")
    df_read = spark.read.parquet(minio_path)
    df_read.show()
    
    # 4. Write to PostgreSQL
    print("🐘 4. Writing to PostgreSQL (smartcity table: staging.test_spark_integration)...")
    postgres_url = "jdbc:postgresql://postgres:5432/smartcity"
    postgres_properties = {
        "user": "smartcity",
        "password": "smartcity",
        "driver": "org.postgresql.Driver"
    }
    table_name = "staging.test_spark_integration"
    
    try:
        df_read.write.jdbc(
            url=postgres_url,
            table=table_name,
            mode="overwrite",
            properties=postgres_properties
        )
        print("✅ Successfully wrote to PostgreSQL.")
    except Exception as e:
        print(f"❌ Failed to write to PostgreSQL: {e}")
        spark.stop()
        raise e
    
    print("🎉 Integration test completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
