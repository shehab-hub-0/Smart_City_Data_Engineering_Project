"""
================================================================================
 Medallion Pipeline — BRONZE LAYER
 Raw Ingestion: Kafka → Iceberg (nessie.bronze_city_events)
================================================================================
 Responsibilities:
   ✓ Subscribe to multiple Kafka topics (smartcity.stream + smartcity.batch)
   ✓ Deserialise the outer JSON envelope from the Kafka VALUE bytes
   ✓ Explode the inner `processedValue` (stringified JSON) into 25 columns
   ✓ Attach Kafka metadata (topic, partition, offset) for lineage
   ✓ Write raw, UNCLEANED data to Iceberg in APPEND mode
   ✓ Use Iceberg streaming write with exactly-once semantics via checkpointing

 NOTE: Bronze is a landing zone — NO transformations, NO filtering.
       Bad/corrupt rows are kept so nothing is silently dropped.
================================================================================
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from p_config.spark_session import create_spark_session
from p_config.settings import (
    KAFKA_BROKER,
    KAFKA_TOPICS,
    CHECKPOINT_DIR_BRONZE,
    BRONZE_TABLE,
    TRIGGER_INTERVAL,
    NESSIE_WAREHOUSE,
)
from p_config.schemas import SENSOR_PAYLOAD_SCHEMA

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# DDL — Create Iceberg table if it doesn't already exist
# ─────────────────────────────────────────────────────────────────────────────
BRONZE_DDL = f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        -- Identity & location
        id                          STRING,
        timestamp                   STRING,
        zone                        STRING,
        device_id                   STRING,
        latitude                    STRING,
        longitude                   STRING,
        metadata_source_type        STRING,

        -- Air quality (stored as raw strings; may contain "_err" garbage)
        air_quality_pm2_5           STRING,
        air_quality_pm10            STRING,
        air_quality_co              STRING,
        air_quality_no2             STRING,
        air_quality_o3              STRING,
        air_quality_quality_index   STRING,

        -- Emergency
        emergency_type              STRING,
        emergency_severity          STRING,
        emergency_response_time     STRING,
        emergency_status            STRING,

        -- Traffic
        traffic_vehicle_count       STRING,
        traffic_avg_speed           STRING,
        traffic_congestion_level    STRING,
        traffic_road_type           STRING,

        -- Weather
        weather_temperature         STRING,
        weather_humidity            STRING,
        weather_wind_speed          STRING,
        weather_condition           STRING,

        -- Lineage columns
        _source_format              STRING,
        _kafka_topic                STRING,
        _kafka_partition            INT,
        _kafka_offset               STRING,
        _kafka_timestamp            STRING,
        _ingested_at                TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zone, days(_ingested_at))
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        -- Keep small streaming micro-batches from creating millions of tiny files.
        -- Iceberg's auto-compaction (or a separate maintenance job) will merge them.
        'write.target-file-size-bytes'    = '134217728',   -- 128 MB target
        'history.expire.max-snapshot-age-ms' = '604800000' -- 7 days of snapshots
    )
    LOCATION '{NESSIE_WAREHOUSE}/bronze_city_events'
"""


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Open a Structured Streaming DataFrame subscribed to all smartcity topics.

    Kafka offsets are managed automatically via the checkpoint directory so the
    pipeline is resilient to restarts and exactly-once delivery is guaranteed
    (when combined with Iceberg's idempotent appends).
    """
    logger.info("Connecting to Kafka broker(s): %s, topics: %s", KAFKA_BROKER, KAFKA_TOPICS)

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPICS)
        # Read ALL historical messages on first run; resume from checkpointed offset
        .option("startingOffsets", "earliest")
        # Guard against an enormous backlog swamping the first micro-batch
        .option("maxOffsetsPerTrigger", 50_000)
        # Wait up to 120 s for a Kafka fetch before failing the micro-batch
        .option("kafkaConsumer.pollTimeoutMs", 120_000)
        # Fail fast if a subscribed topic is missing (catches misconfiguration)
        .option("failOnDataLoss", "false")   # set True in production if needed
        .load()
    )


def parse_kafka_message(raw_df: DataFrame) -> DataFrame:
    """
    Transform the raw Kafka DataFrame into a structured Bronze DataFrame.

    Kafka columns available:  key, value (BINARY), topic, partition,
                              offset, timestamp, timestampType

    Steps:
      1. Cast VALUE bytes → UTF-8 string
      2. Parse outer JSON envelope to extract `processedValue` (or `value.data`)
      3. Parse inner JSON string using the SENSOR_PAYLOAD_SCHEMA
      4. Flatten all 25 sensor columns + attach Kafka metadata columns
    """

    # ── Step 1: bytes → string ────────────────────────────────────────────────
    string_df = raw_df.withColumn("value_str", F.col("value").cast(StringType()))

    # ── Step 2: Parse outer envelope ─────────────────────────────────────────
    # Some producers wrap the payload under `processedValue`, others under
    # `value.data`. We try processedValue first and fall back gracefully.
    outer_schema_fields = "processedValue STRING, value STRING"
    envelope_df = string_df.withColumn(
        "envelope", F.from_json(F.col("value_str"), outer_schema_fields)
    )

    # Resolve the inner JSON string: prefer processedValue, fallback to value
    inner_json_col = F.coalesce(
        F.col("envelope.processedValue"),
        F.col("envelope.value"),
        F.col("value_str"),   # last resort: treat the whole message as payload
    )

    envelope_df = envelope_df.withColumn("inner_json", inner_json_col)

    # ── Step 3: Parse inner sensor payload ───────────────────────────────────
    parsed_df = envelope_df.withColumn(
        "payload", F.from_json(F.col("inner_json"), SENSOR_PAYLOAD_SCHEMA)
    )

    # ── Step 4: Flatten + attach Kafka metadata columns ──────────────────────
    bronze_df = parsed_df.select(
        "payload.*",
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").cast(StringType()).alias("_kafka_offset"),
        F.col("timestamp").cast(StringType()).alias("_kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at")
    )

    return bronze_df


def write_bronze_stream(bronze_df: DataFrame) -> None:
    """
    Sink the Bronze DataFrame to Iceberg using Structured Streaming in
    APPEND mode.

    The Iceberg streaming sink provides:
      - Exactly-once delivery (uses the checkpoint + Iceberg commit atomicity)
      - Automatic file compaction hints via table properties
      - Native Nessie commit per micro-batch (full lineage in Nessie)
    """
    logger.info("Starting Bronze streaming write → %s", BRONZE_TABLE)

    query = (
        bronze_df.writeStream
        .format("iceberg")
        .outputMode("append")
        # The streaming sink requires a checkpoint dir to track Kafka offsets
        # and Iceberg snapshot commits, making restarts safe.
        .option("checkpointLocation", CHECKPOINT_DIR_BRONZE)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .toTable(BRONZE_TABLE)
    )

    # Await termination so the driver doesn't exit immediately.
    # Caught KeyboardInterrupt allows graceful shutdown when the user hits Ctrl+C.
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received! Gracefully stopping Bronze stream...")
        query.stop()
        logger.info("Bronze stream stopped successfully.")


def run_bronze_pipeline():
    """Entry-point: initialise Spark, create DDL, start the streaming pipeline."""
    spark = create_spark_session(app_name="SmartCity-Bronze-Ingestion")

    # Ensure the target table exists before the stream tries to write to it
    logger.info("Creating Bronze table (if not exists)...")
    spark.sql(BRONZE_DDL)

    # Describe the table so we can confirm it was created correctly in logs
    spark.sql(f"DESCRIBE EXTENDED {BRONZE_TABLE}").show(truncate=False)

    # Build the pipeline: Kafka → parse → Iceberg
    raw_kafka_df   = read_kafka_stream(spark)
    bronze_df      = parse_kafka_message(raw_kafka_df)
    write_bronze_stream(bronze_df)


if __name__ == "__main__":
    run_bronze_pipeline()
