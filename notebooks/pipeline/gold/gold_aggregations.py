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
    SILVER_FACT_TABLE,
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
# DDL — Gold table 1: Hourly zone metrics (Enhanced with more KPIs)
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
        no2_o3_sensor_count     BIGINT,

        -- Traffic aggregates
        total_vehicles          BIGINT,
        avg_speed_kmh           DOUBLE,
        congestion_level_mode   STRING,

        -- Record & Sensor counts
        event_count             BIGINT,
        unique_sensor_count     BIGINT,         -- KPI: Active hardware in zone
        
        -- Advanced KPIs
        traffic_efficiency      DOUBLE,         -- speed / vehicles ratio
        infrastructure_load     DOUBLE,         -- KPI: Load percentage
        environmental_index     DOUBLE,         -- combined pollution metric
        air_quality_category    STRING,         -- 'Good', 'Moderate', 'Hazardous'
        weather_impact_score    DOUBLE,         -- Impact on mobility (0-100)
        safety_score            DOUBLE,         -- aggregated risk metric (0-100)
        data_reliability_pct    DOUBLE,         -- KPI: Avg Data Quality Score
        
        last_updated_at         TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zone, days(window_start))
    TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
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
# Batch aggregation: Hourly zone metrics
# ─────────────────────────────────────────────────────────────────────────────
def compute_hourly_zone_metrics(silver_df: DataFrame) -> DataFrame:
    """
    Tumbling 1-hour window aggregations per zone.
    """

    # Calculate mode of congestion_level
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
            F.avg("weather_temperature").alias("avg_temperature_c"),
            F.min("weather_temperature").alias("min_temperature_c"),
            F.max("weather_temperature").alias("max_temperature_c"),
            F.avg("weather_humidity").alias("avg_humidity_pct"),
            F.avg("weather_wind_speed").alias("avg_wind_speed"),
            F.avg("air_quality_pm2_5").alias("avg_pm2_5"),
            F.avg("air_quality_pm10").alias("avg_pm10"),
            F.avg("air_quality_co").alias("avg_co"),
            F.avg("air_quality_no2").alias("avg_no2"),
            F.avg("air_quality_o3").alias("avg_o3"),
            F.sum(F.when(F.col("air_quality_no2").isNotNull(), 1).otherwise(0)).alias("no2_o3_sensor_count"),
            F.avg("air_quality_quality_index").alias("avg_quality_index"),
            F.sum("traffic_vehicle_count").alias("total_vehicles"),
            F.avg("traffic_avg_speed").alias("avg_speed_kmh"),
            F.count("*").alias("event_count"),
            F.countDistinct("device_id").alias("unique_sensor_count"), # New KPI
            F.avg("data_quality_score").alias("data_reliability_pct")   # New KPI
        )
        .withColumn("window_start", F.col("win.start"))
        .withColumn("window_end",   F.col("win.end"))
        .drop("win")
    )

    # Join and calculate complex KPIs
    result = (
        hourly_agg
        .join(
            top_congestion.withColumn("window_start", F.col("win.start")).drop("win"),
            on=["zone", "window_start"],
            how="left"
        )
        .withColumn("environmental_index", 
            F.round((F.col("avg_pm2_5") * 0.4) + (F.col("avg_pm10") * 0.3) + (F.col("avg_co") * 0.3), 2))
        .withColumn("air_quality_category",
            F.when(F.col("avg_quality_index") <= 50, "Good")
             .when(F.col("avg_quality_index") <= 100, "Moderate")
             .otherwise("Hazardous"))
        .withColumn("infrastructure_load", 
            F.round(F.col("total_vehicles") / (F.col("avg_speed_kmh") + 1), 2)) # KPI: Pressure
        .withColumn("traffic_efficiency",
            F.round(F.col("avg_speed_kmh") / (F.col("total_vehicles") + 1), 2))
        .withColumn("weather_impact_score",
            F.round(F.when(F.col("avg_wind_speed") > 50, 50).otherwise(10), 2))
        .withColumn("safety_score",
            F.round(100 - (F.col("environmental_index") + (F.col("infrastructure_load") * 2)), 2))
        .withColumn("last_updated_at", F.current_timestamp())
    )

    return result


def compute_emergency_analysis(silver_df: DataFrame) -> DataFrame:
    emergency_df = silver_df.filter(F.col("emergency_type").isNotNull() & (F.col("emergency_type") != "unknown"))
    agg_df = (
        emergency_df
        .groupBy("zone", "emergency_type", "emergency_severity")
        .agg(
            F.avg("emergency_response_time").alias("avg_response_time_sec"),
            F.min("emergency_response_time").alias("min_response_time_sec"),
            F.max("emergency_response_time").alias("max_response_time_sec"),
            F.percentile_approx("emergency_response_time", 0.95).alias("p95_response_time_sec"),
            F.count("*").alias("total_incidents"),
            F.sum(F.when(F.upper(F.col("emergency_status")) == "ACTIVE", 1).otherwise(0)).alias("active_incidents"),
            F.sum(F.when(F.upper(F.col("emergency_status")) == "RESOLVED", 1).otherwise(0)).alias("resolved_incidents"),
            F.min("event_timestamp").alias("earliest_event"),
            F.max("event_timestamp").alias("latest_event"),
        )
        .withColumn("last_updated_at", F.current_timestamp())
        .withColumn("avg_response_time_sec", F.round("avg_response_time_sec", 2))
    )
    return agg_df


def upsert_hourly_metrics(spark: SparkSession, new_data: DataFrame) -> None:
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
            target.no2_o3_sensor_count   = source.no2_o3_sensor_count,
            target.total_vehicles        = source.total_vehicles,
            target.avg_speed_kmh         = source.avg_speed_kmh,
            target.congestion_level_mode = source.congestion_level_mode,
            target.event_count           = source.event_count,
            target.unique_sensor_count   = source.unique_sensor_count,
            target.traffic_efficiency    = source.traffic_efficiency,
            target.infrastructure_load   = source.infrastructure_load,
            target.environmental_index   = source.environmental_index,
            target.air_quality_category  = source.air_quality_category,
            target.weather_impact_score  = source.weather_impact_score,
            target.safety_score          = source.safety_score,
            target.data_reliability_pct  = source.data_reliability_pct,
            target.last_updated_at       = source.last_updated_at

        WHEN NOT MATCHED THEN INSERT *
    """
    new_data.sparkSession.sql(merge_sql)


def upsert_emergency_analysis(spark: SparkSession, new_data: DataFrame) -> None:
    new_data.createOrReplaceTempView("_new_emergency_analysis")
    merge_sql = f"""
        MERGE INTO {GOLD_EMERGENCY_TABLE} AS target
        USING _new_emergency_analysis AS source
        ON  target.zone               = source.zone
        AND target.emergency_type     = source.emergency_type
        AND COALESCE(target.emergency_severity, '') = COALESCE(source.emergency_severity, '')

        WHEN MATCHED THEN UPDATE SET
            target.avg_response_time_sec = source.avg_response_time_sec,
            target.total_incidents       = source.total_incidents,
            target.active_incidents      = source.active_incidents,
            target.resolved_incidents    = source.resolved_incidents,
            target.earliest_event        = source.earliest_event,
            target.latest_event          = source.latest_event,
            target.last_updated_at       = source.last_updated_at

        WHEN NOT MATCHED THEN INSERT *
    """
    new_data.sparkSession.sql(merge_sql)


def run_gold_batch_pipeline():
    """
    Incremental Batch Mode (Trigger.AvailableNow)
    Processes all available new data since the last run (batch or streaming),
    commits to Iceberg, and then shuts down cleanly.
    """
    spark = create_spark_session(app_name="SmartCity-Gold-Batch-Incremental")
    spark.sql(GOLD_HOURLY_DDL)
    spark.sql(GOLD_EMERGENCY_DDL)

    silver_stream = (
        spark.readStream.format("iceberg")
        .option("stream-from-timestamp", "0")
        .load(SILVER_FACT_TABLE)
        .withWatermark("event_timestamp", WATERMARK_DELAY)
    )

    print("Starting Gold Incremental Batch... processing all new records since last run.")
    h_query = (silver_stream.writeStream.foreachBatch(_hourly_foreach_batch(spark))
              .option("checkpointLocation", CHECKPOINT_DIR_GOLD_HOURLY)
              .trigger(availableNow=True)
              .start())
    
    e_query = (silver_stream.writeStream.foreachBatch(_emergency_foreach_batch(spark))
              .option("checkpointLocation", CHECKPOINT_DIR_GOLD_EMERGENCY)
              .trigger(availableNow=True)
              .start())

    h_query.awaitTermination()
    e_query.awaitTermination()
    print("Gold Incremental Batch completed successfully!")

def _hourly_foreach_batch(spark: SparkSession):
    def _process_batch(micro_batch_df: DataFrame, batch_id: int):
        if micro_batch_df.isEmpty(): return
        hourly_df = compute_hourly_zone_metrics(micro_batch_df)
        upsert_hourly_metrics(spark, hourly_df)
    return _process_batch


def _emergency_foreach_batch(spark: SparkSession):
    def _process_batch(micro_batch_df: DataFrame, batch_id: int):
        if micro_batch_df.isEmpty(): return
        emergency_df = compute_emergency_analysis(micro_batch_df)
        upsert_emergency_analysis(spark, emergency_df)
    return _process_batch


def run_gold_streaming_pipeline():
    spark = create_spark_session(app_name="SmartCity-Gold-Streaming")
    spark.sql(GOLD_HOURLY_DDL)
    spark.sql(GOLD_EMERGENCY_DDL)

    silver_stream = (
        spark.readStream.format("iceberg")
        .option("stream-from-timestamp", "0")
        .load(SILVER_FACT_TABLE)
        .withWatermark("event_timestamp", WATERMARK_DELAY)
    )

    h_query = (silver_stream.writeStream.foreachBatch(_hourly_foreach_batch(spark))
              .option("checkpointLocation", CHECKPOINT_DIR_GOLD_HOURLY).start())
    e_query = (silver_stream.writeStream.foreachBatch(_emergency_foreach_batch(spark))
              .option("checkpointLocation", CHECKPOINT_DIR_GOLD_EMERGENCY).start())

    try:
        h_query.awaitTermination()
        e_query.awaitTermination()
    except KeyboardInterrupt:
        print("KeyboardInterrupt received! Gracefully stopping Gold streams...")
        h_query.stop()
        e_query.stop()
        print("Gold streams stopped successfully.")


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    if mode == "streaming":
        run_gold_streaming_pipeline()
    else:
        run_gold_batch_pipeline()
