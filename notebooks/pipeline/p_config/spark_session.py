"""
================================================================================
 Medallion Architecture Pipeline — Spark Session Initialization
 Tech Stack: PySpark + Apache Iceberg + Project Nessie + MinIO + Kafka
================================================================================
 This module builds a fully configured SparkSession with:
   - Iceberg table format support (runtime JAR + extensions)
   - Nessie as the Iceberg REST catalog
   - MinIO as the S3-compatible object store (via s3a://)
   - Kafka connector for Structured Streaming
================================================================================
"""

from pyspark.sql import SparkSession
from p_config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    NESSIE_URL,
    NESSIE_DEFAULT_BRANCH,
    NESSIE_WAREHOUSE,
)


def create_spark_session(app_name: str = "SmartCity-Medallion-Pipeline") -> SparkSession:
    """
    Build and return a SparkSession configured for:
      - Apache Iceberg (with Nessie catalog)
      - MinIO / S3-compatible storage
      - Kafka Structured Streaming

    JAR dependencies (must be available on the Spark cluster or passed via
    --packages / spark.jars.packages):
      - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
      - org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3
      - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
      - org.apache.hadoop:hadoop-aws:3.3.4
      - com.amazonaws:aws-java-sdk-bundle:1.12.262
    """

    spark = (
        SparkSession.builder
        .appName(app_name)

        # ── Memory — fit inside the container's 1.5 GB cgroup limit ────────
        .config("spark.driver.memory", "512m")

        # ── Iceberg + Nessie extensions ───────────────────────────────────────
        # Register the Iceberg SparkSessionExtensions so that Iceberg-specific
        # SQL (CREATE TABLE … USING iceberg, MERGE INTO, etc.) works natively.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )

        # ── Nessie Catalog (named "nessie") ───────────────────────────────────
        # All tables written as  nessie.<table>  will use this catalog.
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URL)
        .config("spark.sql.catalog.nessie.ref", NESSIE_DEFAULT_BRANCH)
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")  # change for prod auth
        .config("spark.sql.catalog.nessie.warehouse", NESSIE_WAREHOUSE)
        # Use HadoopFileIO to leverage the s3a:// filesystem already configured
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # Ensure 's3://' and 's3a://' both use the S3AFileSystem implementation
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.catalog.nessie.s3.endpoint", MINIO_ENDPOINT)
        # Path-style access is REQUIRED for MinIO (not virtual-hosted style)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")

        # ── Hadoop / s3a connector for MinIO ─────────────────────────────────
        # These settings configure the underlying Hadoop FileSystem used for
        # all s3a:// paths (checkpoints, warehouse, etc.)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Use the SimpleAWSCredentialsProvider so we don't need EC2/IAM metadata
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        # Optimise s3a for small-file writes typical in streaming workloads
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")        # 100 MB
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")

        # ── Iceberg runtime JARs ──────────────────────────────────────────────
        # Provide all required JARs via spark.jars.packages (resolved from Maven)
        # or pre-stage them and use spark.jars for air-gapped environments.
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )

        # ── Shuffle & performance tuning ──────────────────────────────────────
        # Adaptive Query Execution — good for dynamic skew in streaming
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Keep shuffle partitions modest for a dev/single-node setup;
        # increase for production clusters.
        .config("spark.sql.shuffle.partitions", "2")

        # ── Iceberg write defaults ────────────────────────────────────────────
        # Use "merging" write distribution mode for better file sizing
        .config("spark.sql.iceberg.write.distribution-mode", "hash")

        # ── Streaming micro-batch settings ────────────────────────────────────
        # Max records Spark pulls per Kafka partition per trigger.
        # Tune this to control throughput vs. latency.
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")

        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    # Set log level — use WARN in production to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    return spark
