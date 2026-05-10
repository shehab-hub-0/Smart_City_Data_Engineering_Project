"""
================================================================================
 Medallion Pipeline — GOLD LAYER
 Business Aggregations: Silver → Two Gold Tables (for Dremio BI)
================================================================================
 Produces two Gold Iceberg tables designed for dashboards:

   1. nessie.gold_hourly_zone_metrics
      → Tumbling 1-hour window per zone: Avg Temp, Avg PM2.5, Total Vehicles
      → Updated via MERGE INTO (upsert) keyed on (zone, window_start)

   2. nessie.gold_emergency_analysis
      → Aggregated emergency response times & severity counts per zone
      → Updated via MERGE INTO keyed on (zone, emergency_type, emergency_severity)

 Both tables are written as batch jobs triggered on a schedule (e.g., every
 15 minutes via Airflow / a cron spark-submit). For near-real-time use, the
 foreachBatch pattern with streaming + MERGE INTO is also shown.
================================================================================
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from p_config.spark_session import create_spark_session
from p_config.settings import (
    SILVER_TABLE,
    GOLD_HOURLY_TABLE,
    GOLD_EMERGENCY_TABLE,
    CHECKPOINT_DIR_GOLD_HOURLY,
    CHECKPOINT_DIR_GOLD_EMERGENCY,
    GOLD_WINDOW_DURATION,
    WATERMARK_DELAY,
    TRIGGER_INTERVAL,
    NESSIE_WAREHOUSE,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# DDL — Gold table 1: Hourly zone metrics
# ─────────────────────────────────────────────────────────────────────────────
GOLD_HOURLY_DDL = f"""
    CREATE TABLE IF NOT EXISTS {GOLD_HOURLY_TABLE} (
        zone                    STRING,
        window_start            TIMESTAMP,
        window_end              TIMESTAMP,

        -- Weather aggregates
        avg_temperature_c       DOUBLE,
        min_temperature_c       DOUBLE,
        max_temperature_c       DOUBLE,
        avg_humidity_pct        DOUBLE,
        avg_wind_speed          DOUBLE,

        -- Air quality aggregates
        avg_pm2_5               DOUBLE,
        avg_pm10                DOUBLE,
        avg_co                  DOUBLE,
        avg_no2                 DOUBLE,
        avg_o3                  DOUBLE,
        avg_quality_index       DOUBLE,

        -- Traffic aggregates
        total_vehicles          BIGINT,
        avg_speed_kmh           DOUBLE,
        congestion_level_mode   STRING,         -- most frequent congestion level

        -- Record counts
        event_count             BIGINT,
        last_updated_at         TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zone, days(window_start))
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes'    = '268435456',
        -- Row-level deletes for MERGE INTO efficiency
        'write.delete.mode'               = 'copy-on-write',
        'write.update.mode'               = 'copy-on-write',
        'write.merge.mode'                = 'copy-on-write'
    )
    LOCATION '{NESSIE_WAREHOUSE}/gold_hourly_zone_metrics'
"""

# ─────────────────────────────────────────────────────────────────────────────
# DDL — Gold table 2: Emergency analysis
# ─────────────────────────────────────────────────────────────────────────────
GOLD_EMERGENCY_DDL = f"""
    CREATE TABLE IF NOT EXISTS {GOLD_EMERGENCY_TABLE} (
        zone                        STRING,
        emergency_type              STRING,
        emergency_severity          STRING,

        -- Response time statistics
        avg_response_time_sec       DOUBLE,
        min_response_time_sec       DOUBLE,
        max_response_time_sec       DOUBLE,
        p95_response_time_sec       DOUBLE,         -- 95th percentile

        -- Volume
        total_incidents             BIGINT,
        active_incidents            BIGINT,         -- status = ACTIVE
        resolved_incidents          BIGINT,         -- status = RESOLVED

        -- Time range covered by this aggregate
        earliest_event              TIMESTAMP,
        latest_event                TIMESTAMP,

        last_updated_at             TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zone)
    TBLPROPERTIES (
        'format-version'        = '2',
        'write.format.default'  = 'parquet',
        'write.delete.mode'     = 'copy-on-write',
        'write.update.mode'     = 'copy-on-write',
        'write.merge.mode'      = 'copy-on-write'
    )
    LOCATION '{NESSIE_WAREHOUSE}/gold_emergency_analysis'
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helper: most-frequent value within a group (mode)
# ─────────────────────────────────────────────────────────────────────────────
def mode_udf(col_name: str, partition_cols: list) -> F.Column:
    """
    Compute the statistical mode of `col_name` within each partition defined
    by `partition_cols` using a Window + rank approach.

    Returns the most frequently occurring value per group, ties broken by
    first alphabetically.
    """
    count_window = Window.partitionBy(*partition_cols, col_name)
    rank_window  = Window.partitionBy(*partition_cols).orderBy(
        F.desc("_mode_count"), col_name
    )
    return (
        F.first(col_name).over(
            rank_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Batch aggregation: Hourly zone metrics
# ─────────────────────────────────────────────────────────────────────────────
def compute_hourly_zone_metrics(silver_df: DataFrame) -> DataFrame:
    """
    Tumbling 1-hour window aggregations per zone.

    For streaming: call this inside foreachBatch.
    For batch:     call directly on the Silver table DataFrame.
    """

    # Calculate mode of congestion_level per (zone, hour window, congestion_level)
    # using a helper column approach (no UDF needed)
    congestion_counts = (
        silver_df
        .filter(F.col("traffic_congestion_level").isNotNull())
        .groupBy(
            F.col("zone"),
            F.window(F.col("event_timestamp"), GOLD_WINDOW_DURATION).alias("win"),
            F.col("traffic_congestion_level")
        )
        .agg(F.count("*").alias("_cong_count"))
    )

    # Rank congestion levels within each (zone, window), pick the top one
    rank_window = Window.partitionBy("zone", "win").orderBy(F.desc("_cong_count"))
    top_congestion = (
        congestion_counts
        .withColumn("_rank", F.rank().over(rank_window))
        .filter(F.col("_rank") == 1)
        .select(
            "zone", "win",
            F.col("traffic_congestion_level").alias("congestion_level_mode")
        )
    )

    # Main hourly aggregation
    hourly_agg = (
        silver_df
        .groupBy(
            F.col("zone"),
            F.window(F.col("event_timestamp"), GOLD_WINDOW_DURATION).alias("win")
        )
        .agg(
            # Weather
            F.avg("weather_temperature").alias("avg_temperature_c"),
            F.min("weather_temperature").alias("min_temperature_c"),
            F.max("weather_temperature").alias("max_temperature_c"),
            F.avg("weather_humidity").alias("avg_humidity_pct"),
            F.avg("weather_wind_speed").alias("avg_wind_speed"),

            # Air quality
            F.avg("air_quality_pm2_5").alias("avg_pm2_5"),
            F.avg("air_quality_pm10").alias("avg_pm10"),
            F.avg("air_quality_co").alias("avg_co"),
            F.avg("air_quality_no2").alias("avg_no2"),
            F.avg("air_quality_o3").alias("avg_o3"),
            F.avg("air_quality_quality_index").alias("avg_quality_index"),

            # Traffic
            F.sum("traffic_vehicle_count").alias("total_vehicles"),
            F.avg("traffic_avg_speed").alias("avg_speed_kmh"),

            # Meta
            F.count("*").alias("event_count"),
        )
        .withColumn("window_start", F.col("win.start"))
        .withColumn("window_end",   F.col("win.end"))
        .drop("win")
    )

    # Join in the congestion mode
    result = (
        hourly_agg
        .join(
            top_congestion.withColumn("window_start", F.col("win.start")).drop("win"),
            on=["zone", "window_start"],
            how="left"
        )
        .withColumn("last_updated_at", F.current_timestamp())
        # Round doubles for cleaner BI display
        .withColumn("avg_temperature_c",  F.round("avg_temperature_c",  2))
        .withColumn("avg_humidity_pct",   F.round("avg_humidity_pct",   2))
        .withColumn("avg_wind_speed",     F.round("avg_wind_speed",     2))
        .withColumn("avg_pm2_5",          F.round("avg_pm2_5",          3))
        .withColumn("avg_pm10",           F.round("avg_pm10",           3))
        .withColumn("avg_co",             F.round("avg_co",             4))
        .withColumn("avg_no2",            F.round("avg_no2",            4))
        .withColumn("avg_o3",             F.round("avg_o3",             4))
        .withColumn("avg_speed_kmh",      F.round("avg_speed_kmh",      2))
    )

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Batch aggregation: Emergency analysis
# ─────────────────────────────────────────────────────────────────────────────
def compute_emergency_analysis(silver_df: DataFrame) -> DataFrame:
    """
    Emergency incident aggregation per (zone, emergency_type, emergency_severity).

    p95 is approximated with percentile_approx — exact percentile would require
    a full sort which is expensive in large distributed datasets.
    """

    emergency_df = silver_df.filter(F.col("emergency_type").isNotNull())

    agg_df = (
        emergency_df
        .groupBy("zone", "emergency_type", "emergency_severity")
        .agg(
            # Response time stats
            F.avg("emergency_response_time").alias("avg_response_time_sec"),
            F.min("emergency_response_time").alias("min_response_time_sec"),
            F.max("emergency_response_time").alias("max_response_time_sec"),
            F.percentile_approx("emergency_response_time", 0.95)
              .alias("p95_response_time_sec"),

            # Volume
            F.count("*").alias("total_incidents"),
            F.sum(
                F.when(F.upper(F.col("emergency_status")) == "ACTIVE", 1).otherwise(0)
            ).alias("active_incidents"),
            F.sum(
                F.when(F.upper(F.col("emergency_status")) == "RESOLVED", 1).otherwise(0)
            ).alias("resolved_incidents"),

            # Time range
            F.min("event_timestamp").alias("earliest_event"),
            F.max("event_timestamp").alias("latest_event"),
        )
        .withColumn("last_updated_at", F.current_timestamp())
        .withColumn("avg_response_time_sec", F.round("avg_response_time_sec", 2))
        .withColumn("p95_response_time_sec", F.round("p95_response_time_sec", 2))
    )

    return agg_df


# ─────────────────────────────────────────────────────────────────────────────
# MERGE INTO helpers (Iceberg upsert)
# ─────────────────────────────────────────────────────────────────────────────
def upsert_hourly_metrics(spark: SparkSession, new_data: DataFrame) -> None:
    """
    MERGE INTO nessie.gold_hourly_zone_metrics using the incoming aggregated
    DataFrame.  Matched rows are UPDATED, unmatched rows are INSERTED.

    We use a temp view as the source for the MERGE statement, which is the
    cleanest way to drive MERGE from a DataFrame in PySpark.
    """
    new_data.createOrReplaceTempView("_new_hourly_metrics")

    merge_sql = f"""
        MERGE INTO {GOLD_HOURLY_TABLE} AS target
        USING _new_hourly_metrics AS source
        ON  target.zone         = source.zone
        AND target.window_start = source.window_start

        WHEN MATCHED THEN UPDATE SET
            target.window_end            = source.window_end,
            target.avg_temperature_c     = source.avg_temperature_c,
            target.min_temperature_c     = source.min_temperature_c,
            target.max_temperature_c     = source.max_temperature_c,
            target.avg_humidity_pct      = source.avg_humidity_pct,
            target.avg_wind_speed        = source.avg_wind_speed,
            target.avg_pm2_5             = source.avg_pm2_5,
            target.avg_pm10              = source.avg_pm10,
            target.avg_co                = source.avg_co,
            target.avg_no2               = source.avg_no2,
            target.avg_o3                = source.avg_o3,
            target.avg_quality_index     = source.avg_quality_index,
            target.total_vehicles        = source.total_vehicles,
            target.avg_speed_kmh         = source.avg_speed_kmh,
            target.congestion_level_mode = source.congestion_level_mode,
            target.event_count           = source.event_count,
            target.last_updated_at       = source.last_updated_at

        WHEN NOT MATCHED THEN INSERT *
    """

    logger.info("Running MERGE INTO for hourly zone metrics...")
    spark.sql(merge_sql)
    logger.info("MERGE INTO %s completed.", GOLD_HOURLY_TABLE)


def upsert_emergency_analysis(spark: SparkSession, new_data: DataFrame) -> None:
    """
    MERGE INTO nessie.gold_emergency_analysis.

    Merge key: (zone, emergency_type, emergency_severity).
    Running counts & p95 are fully replaced — recalculating from Silver is
    easier than incrementally updating percentiles.
    """
    new_data.createOrReplaceTempView("_new_emergency_analysis")

    merge_sql = f"""
        MERGE INTO {GOLD_EMERGENCY_TABLE} AS target
        USING _new_emergency_analysis AS source
        ON  target.zone               = source.zone
        AND target.emergency_type     = source.emergency_type
        AND COALESCE(target.emergency_severity, '') =
            COALESCE(source.emergency_severity, '')

        WHEN MATCHED THEN UPDATE SET
            target.avg_response_time_sec = source.avg_response_time_sec,
            target.min_response_time_sec = source.min_response_time_sec,
            target.max_response_time_sec = source.max_response_time_sec,
            target.p95_response_time_sec = source.p95_response_time_sec,
            target.total_incidents       = source.total_incidents,
            target.active_incidents      = source.active_incidents,
            target.resolved_incidents    = source.resolved_incidents,
            target.earliest_event        = source.earliest_event,
            target.latest_event          = source.latest_event,
            target.last_updated_at       = source.last_updated_at

        WHEN NOT MATCHED THEN INSERT *
    """

    logger.info("Running MERGE INTO for emergency analysis...")
    spark.sql(merge_sql)
    logger.info("MERGE INTO %s completed.", GOLD_EMERGENCY_TABLE)


# ─────────────────────────────────────────────────────────────────────────────
# OPTION A — Batch Gold job (recommended for most use cases)
# Run this as a scheduled Spark job every N minutes via Airflow / cron.
# ─────────────────────────────────────────────────────────────────────────────
def run_gold_batch_pipeline():
    """
    Batch Gold pipeline:
      1. Read the FULL Silver table (or last N hours using partition pruning)
      2. Compute both aggregations
      3. MERGE INTO the Gold tables

    Partition pruning: we filter Silver to the last 25 hours so we reprocess
    only the windows that could still receive late data (watermark = 10 minutes
    but we add a safety margin).
    """
    spark = create_spark_session(app_name="SmartCity-Gold-Batch")

    # Create Gold tables if they don't exist
    logger.info("Creating Gold tables (if not exist)...")
    spark.sql(GOLD_HOURLY_DDL)
    spark.sql(GOLD_EMERGENCY_DDL)

    # Read Silver — apply a time-bounded filter to avoid full table scans.
    # Adjust the lookback window based on your SLA and watermark settings.
    lookback_hours = 25
    silver_df = (
        spark.read
        .format("iceberg")
        .load(SILVER_TABLE)
        .filter(
            F.col("event_timestamp") >=
            F.date_add(F.current_timestamp().cast("date"), -lookback_hours // 24)
        )
    )

    logger.info("Silver rows in lookback window: %d", silver_df.count())

    # ── Compute & upsert Hourly Zone Metrics ─────────────────────────────────
    hourly_df = compute_hourly_zone_metrics(silver_df)
    upsert_hourly_metrics(spark, hourly_df)

    # ── Compute & upsert Emergency Analysis ──────────────────────────────────
    emergency_df = compute_emergency_analysis(silver_df)
    upsert_emergency_analysis(spark, emergency_df)

    logger.info("Gold batch pipeline completed successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# OPTION B — Streaming Gold pipeline using foreachBatch
# Use this for near-real-time Gold updates (sub-minute latency).
# ─────────────────────────────────────────────────────────────────────────────
def _hourly_foreach_batch(spark: SparkSession):
    """
    Returns a foreachBatch function for the hourly metrics stream.
    We close over `spark` to access it inside the batch function.
    """
    def _process_batch(micro_batch_df: DataFrame, batch_id: int):
        logger.info("Processing micro-batch %d for hourly Gold...", batch_id)

        if micro_batch_df.rdd.isEmpty():
            logger.info("Batch %d is empty, skipping.", batch_id)
            return

        hourly_df = compute_hourly_zone_metrics(micro_batch_df)
        upsert_hourly_metrics(spark, hourly_df)

    return _process_batch


def _emergency_foreach_batch(spark: SparkSession):
    """Returns a foreachBatch function for the emergency analysis stream."""
    def _process_batch(micro_batch_df: DataFrame, batch_id: int):
        logger.info("Processing micro-batch %d for emergency Gold...", batch_id)

        if micro_batch_df.rdd.isEmpty():
            logger.info("Batch %d is empty, skipping.", batch_id)
            return

        emergency_df = compute_emergency_analysis(micro_batch_df)
        upsert_emergency_analysis(spark, emergency_df)

    return _process_batch


def run_gold_streaming_pipeline():
    """
    Streaming Gold pipeline using foreachBatch + MERGE INTO.

    Reads Silver as a stream and triggers MERGE INTO Gold tables on every
    micro-batch. Both streams run in parallel using different checkpoint dirs.

    Trade-off vs batch: more real-time but higher Iceberg commit pressure.
    Consider increasing trigger interval for large Silver tables.
    """
    spark = create_spark_session(app_name="SmartCity-Gold-Streaming")

    spark.sql(GOLD_HOURLY_DDL)
    spark.sql(GOLD_EMERGENCY_DDL)

    # Read Silver as a stream
    silver_stream = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load(SILVER_TABLE)
        .withWatermark("event_timestamp", WATERMARK_DELAY)
    )

    # ── Stream 1: Hourly zone metrics ─────────────────────────────────────────
    hourly_query = (
        silver_stream.writeStream
        .foreachBatch(_hourly_foreach_batch(spark))
        .option("checkpointLocation", CHECKPOINT_DIR_GOLD_HOURLY)
        .trigger(processingTime="5 minutes")   # coarser trigger reduces commit load
        .start()
    )

    # ── Stream 2: Emergency analysis ─────────────────────────────────────────
    emergency_query = (
        silver_stream.writeStream
        .foreachBatch(_emergency_foreach_batch(spark))
        .option("checkpointLocation", CHECKPOINT_DIR_GOLD_EMERGENCY)
        .trigger(processingTime="5 minutes")
        .start()
    )

    # Wait for both streams
    hourly_query.awaitTermination()
    emergency_query.awaitTermination()


# ─────────────────────────────────────────────────────────────────────────────
# Entry-point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # Allow choosing batch vs streaming mode at runtime:
    #   python gold_aggregations.py batch
    #   python gold_aggregations.py streaming
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"

    if mode == "streaming":
        logger.info("Starting Gold pipeline in STREAMING mode...")
        run_gold_streaming_pipeline()
    else:
        logger.info("Starting Gold pipeline in BATCH mode...")
        run_gold_batch_pipeline()
