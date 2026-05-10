"""
================================================================================
 Medallion Pipeline — SILVER LAYER (Snowflake Schema)
 Cleansing & Conformed: Iceberg Bronze → Iceberg Silver (Normalized)
================================================================================
 Responsibilities:
   ✓ Read Structured Stream from Bronze Iceberg table
   ✓ Apply normalization into Fact and Dimension tables
   ✓ Multi-target upsert via foreachBatch
================================================================================
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from p_config.spark_session import create_spark_session
from p_config.settings import (
    BRONZE_TABLE,
    SILVER_FACT_TABLE,
    SILVER_DIM_LOCATIONS,
    SILVER_DIM_SENSORS,
    CHECKPOINT_DIR_SILVER,
    WATERMARK_DELAY,
    TRIGGER_INTERVAL,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# DDLs — Snowflake Schema (Fact & Dimensions)
# ─────────────────────────────────────────────────────────────────────────────

# 1. Dimension: Locations
DIM_LOCATIONS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DIM_LOCATIONS} (
        zone                STRING,
        latitude            DOUBLE,
        longitude           DOUBLE,
        _last_updated       TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (zone)
"""

# 2. Dimension: Sensors
DIM_SENSORS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DIM_SENSORS} (
        device_id           STRING,
        sensor_type         STRING,
        zone                STRING,
        _last_updated       TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (sensor_type)
"""

# 3. Fact: City Events
FACT_EVENTS_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SILVER_FACT_TABLE} (
        id                          STRING,
        device_id                   STRING,
        event_timestamp             TIMESTAMP,
        zone                        STRING,
        
        -- Measurements
        air_quality_pm2_5           DOUBLE,
        air_quality_pm10            DOUBLE,
        air_quality_co              DOUBLE,
        air_quality_no2             DOUBLE,
        air_quality_o3              DOUBLE,
        air_quality_quality_index   INT,
        emergency_type              STRING,
        emergency_severity          INT,
        emergency_response_time     DOUBLE,
        emergency_status            STRING,
        traffic_vehicle_count       INT,
        traffic_avg_speed           DOUBLE,
        traffic_congestion_level    STRING,
        traffic_road_type           STRING,
        weather_temperature         DOUBLE,
        weather_humidity            DOUBLE,
        weather_wind_speed          DOUBLE,
        weather_condition           STRING,

        -- Metadata
        _ingested_at                TIMESTAMP,
        data_quality_score          DOUBLE,
        has_air_quality_data        BOOLEAN,
        has_traffic_data            BOOLEAN,
        has_emergency_data          BOOLEAN,
        has_weather_data            BOOLEAN
    )
    USING iceberg
    PARTITIONED BY (days(event_timestamp), zone)
    TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read')
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helper: clean a single string column that may contain "_err" noise
# ─────────────────────────────────────────────────────────────────────────────
def clean_numeric_string(col_name: str) -> F.Column:
    """
    Strip any trailing "_err" (or other non-numeric noise) from a string field.
    """
    cleaned = F.regexp_extract(F.col(col_name), r"^(-?[0-9]+\.?[0-9]*)", 1)
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def add_domain_flags(df: DataFrame) -> DataFrame:
    return df \
        .withColumn("has_air_quality_data", F.col("air_quality_pm2_5").isNotNull()) \
        .withColumn("has_traffic_data",     F.col("traffic_vehicle_count").isNotNull()) \
        .withColumn("has_emergency_data",   F.col("emergency_type").isNotNull()) \
        .withColumn("has_weather_data",     F.col("weather_temperature").isNotNull())


# ─────────────────────────────────────────────────────────────────────────────
# Core Transformation Logic
# ─────────────────────────────────────────────────────────────────────────────
def apply_silver_transformations(df: DataFrame) -> DataFrame:
    """
    Full cleansing pipeline applied to each micro-batch.
    """

    # 1. Parse timestamp string → TimestampType
    df = df.withColumn(
        "event_timestamp",
        F.coalesce(
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("timestamp"), "dd-MM-yyyy HH:mm:ss"),
            F.to_timestamp(F.col("timestamp"), "yyyy/MM/dd HH:mm"),
            F.to_timestamp(F.col("timestamp"), "MM-dd-yyyy"),
            (F.col("timestamp").cast("long") / 1000).cast(TimestampType()),
        )
    )

    # 2. Watermark & Deduplicate
    df = (
        df.withWatermark("event_timestamp", WATERMARK_DELAY)
        .dropDuplicates(["id"])
    )

    # 3. Clean numeric strings
    numeric_string_cols = [
        "air_quality_pm2_5", "air_quality_pm10", "air_quality_co",
        "air_quality_no2", "air_quality_o3", "air_quality_quality_index",
        "emergency_response_time", "traffic_vehicle_count", "traffic_avg_speed",
        "weather_temperature", "weather_humidity", "weather_wind_speed",
        "latitude", "longitude"
    ]
    for col_name in numeric_string_cols:
        df = df.withColumn(col_name, clean_numeric_string(col_name))

    # Helper to clean categorical strings
    def clean_category(col_name):
        c = F.lower(F.trim(F.col(col_name)))
        return F.when((c.isNull()) | (c == "nan") | (c == "null"), F.lit("unknown")).otherwise(c)

    # 4. Cast and Standardize
    df = (
        df
        .withColumn("zone", clean_category("zone"))
        .withColumn("device_id", F.coalesce(F.col("device_id"), F.lit("unknown-dev")))
        .withColumn("emergency_type", clean_category("emergency_type"))
        .withColumn("emergency_status", clean_category("emergency_status"))
        .withColumn("traffic_congestion_level", clean_category("traffic_congestion_level"))
        .withColumn("traffic_road_type", clean_category("traffic_road_type"))
        .withColumn("weather_condition", clean_category("weather_condition"))
        .withColumn("latitude",  F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("air_quality_pm2_5", F.col("air_quality_pm2_5").cast(DoubleType()))
        .withColumn("air_quality_pm10",  F.col("air_quality_pm10").cast(DoubleType()))
        .withColumn("air_quality_co",    F.col("air_quality_co").cast(DoubleType()))
        .withColumn("air_quality_no2",   F.col("air_quality_no2").cast(DoubleType()))
        .withColumn("air_quality_o3",    F.col("air_quality_o3").cast(DoubleType()))
        .withColumn("air_quality_quality_index", F.col("air_quality_quality_index").cast(IntegerType()))
        .withColumn("emergency_response_time",   F.col("emergency_response_time").cast(DoubleType()))
        .withColumn("emergency_severity",        F.col("emergency_severity").cast(IntegerType()))
        .withColumn("traffic_vehicle_count",     F.col("traffic_vehicle_count").cast(IntegerType()))
        .withColumn("traffic_avg_speed",         F.col("traffic_avg_speed").cast(DoubleType()))
        .withColumn("weather_temperature",       F.col("weather_temperature").cast(DoubleType()))
        .withColumn("weather_humidity",          F.col("weather_humidity").cast(DoubleType()))
        .withColumn("weather_wind_speed",         F.col("weather_wind_speed").cast(DoubleType()))
    )

    # 4b. Clamp outlier values to valid physical ranges
    #     (fixes sensor drift / GPS glitch from generators)
    df = (
        df
        # Weather: temperature 0–55°C, humidity 0–100%, wind 0–100 km/h
        .withColumn("weather_temperature",
            F.when(F.col("weather_temperature").isNull(), None)
            .otherwise(F.least(F.lit(55.0), F.greatest(F.lit(0.0), F.col("weather_temperature")))))
        .withColumn("weather_humidity",
            F.when(F.col("weather_humidity").isNull(), None)
            .otherwise(F.least(F.lit(100.0), F.greatest(F.lit(0.0), F.col("weather_humidity")))))
        .withColumn("weather_wind_speed",
            F.when(F.col("weather_wind_speed").isNull(), None)
            .otherwise(F.least(F.lit(100.0), F.greatest(F.lit(0.0), F.col("weather_wind_speed")))))
        # Traffic: speed 0–140 km/h, vehicles 0–1000
        .withColumn("traffic_avg_speed",
            F.when(F.col("traffic_avg_speed").isNull(), None)
            .otherwise(F.least(F.lit(140.0), F.greatest(F.lit(0.0), F.col("traffic_avg_speed")))))
        .withColumn("traffic_vehicle_count",
            F.when(F.col("traffic_vehicle_count").isNull(), None)
            .otherwise(F.least(F.lit(1000), F.greatest(F.lit(0), F.col("traffic_vehicle_count")))))
        # Emergency: response_time 0–7200s, severity 1–5
        .withColumn("emergency_response_time",
            F.when(F.col("emergency_response_time").isNull(), None)
            .otherwise(F.least(F.lit(7200.0), F.greatest(F.lit(0.0), F.col("emergency_response_time")))))
        .withColumn("emergency_severity",
            F.when(F.col("emergency_severity").isNull(), None)
            .otherwise(F.least(F.lit(5), F.greatest(F.lit(1), F.col("emergency_severity")))))
        # Air Quality: PM2.5 0–500, PM10 0–1000, AQI 0–500
        .withColumn("air_quality_pm2_5",
            F.when(F.col("air_quality_pm2_5").isNull(), None)
            .otherwise(F.least(F.lit(500.0), F.greatest(F.lit(0.0), F.col("air_quality_pm2_5")))))
        .withColumn("air_quality_pm10",
            F.when(F.col("air_quality_pm10").isNull(), None)
            .otherwise(F.least(F.lit(1000.0), F.greatest(F.lit(0.0), F.col("air_quality_pm10")))))
        .withColumn("air_quality_quality_index",
            F.when(F.col("air_quality_quality_index").isNull(), None)
            .otherwise(F.least(F.lit(500), F.greatest(F.lit(0), F.col("air_quality_quality_index")))))
    )

    # 5. Add Domain Flags
    df = add_domain_flags(df)

    # 6. Data Quality Score (always 0–100)
    null_penalty = (
        F.when(F.col("zone") == "unknown", F.lit(15)).otherwise(F.lit(0)) +
        F.when(F.col("weather_temperature").isNull(), F.lit(10)).otherwise(F.lit(0)) +
        F.when(F.col("air_quality_pm2_5").isNull(), F.lit(10)).otherwise(F.lit(0)) +
        F.when(F.col("event_timestamp").isNull(), F.lit(25)).otherwise(F.lit(0)) +
        F.when(F.col("latitude").isNull() | (F.col("latitude") == 0.0), F.lit(15)).otherwise(F.lit(0))
    )
    df = df.withColumn("data_quality_score",
        F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.round(100.0 - null_penalty, 2)))
    )

    # 7. Sensible defaults & Averages (avoid blanket 0.0 which creates impossible values)
    # Categorical defaults
    df = (
        df
        .fillna({"zone": "unknown", "device_id": "unknown-dev"})
        .fillna({"emergency_type": "unknown", "emergency_status": "unknown"})
        .fillna({"traffic_congestion_level": "unknown", "traffic_road_type": "unknown"})
        .fillna({"weather_condition": "unknown"})
    )
    # Numeric Averages (Imputation for streaming)
    df = (
        df
        .fillna({
            "weather_temperature": 25.0,
            "weather_humidity": 50.0,
            "weather_wind_speed": 15.0,
            "traffic_avg_speed": 45.0,
            "traffic_vehicle_count": 150,
            "emergency_response_time": 1800.0,
            "emergency_severity": 3,
            "air_quality_pm2_5": 50.0,
            "air_quality_pm10": 75.0,
            "air_quality_co": 2.0,
            "air_quality_no2": 40.0,
            "air_quality_o3": 45.0,
            "air_quality_quality_index": 100
        })
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Multi-Target Upsert (foreachBatch)
# ─────────────────────────────────────────────────────────────────────────────
def upsert_to_snowflake(micro_batch_df: DataFrame, batch_id: int):
    """
    Distributes the micro-batch into Dimension and Fact tables using MERGE/INSERT.
    """
    if micro_batch_df.isEmpty():
        return
        
    spark = micro_batch_df.sparkSession
    processed_time = F.current_timestamp()

    # --- 1. Update Dim: Locations ---
    dim_loc = (
        micro_batch_df.select("zone", "latitude", "longitude")
        .groupBy("zone")
        .agg(F.first("latitude").alias("latitude"), F.first("longitude").alias("longitude"))
        .withColumn("_last_updated", processed_time)
    )
    dim_loc.createOrReplaceTempView("batch_dim_loc")
    spark.sql(f"""
        MERGE INTO {SILVER_DIM_LOCATIONS} t USING batch_dim_loc s ON t.zone = s.zone
        WHEN MATCHED THEN UPDATE SET t.latitude = s.latitude, t.longitude = s.longitude, t._last_updated = s._last_updated
        WHEN NOT MATCHED THEN INSERT *
    """)

    # --- 2. Update Dim: Sensors ---
    dim_sens = (
        micro_batch_df.select("device_id", "zone", "has_weather_data", "has_traffic_data", "has_emergency_data")
        .withColumn("sensor_type", 
            F.when(F.col("has_weather_data"), "weather")
            .when(F.col("has_traffic_data"), "traffic")
            .when(F.col("has_emergency_data"), "emergency")
            .otherwise("air_quality"))
        .groupBy("device_id")
        .agg(F.first("sensor_type").alias("sensor_type"), F.first("zone").alias("zone"))
        .withColumn("_last_updated", processed_time)
    )
    dim_sens.createOrReplaceTempView("batch_dim_sens")
    spark.sql(f"""
        MERGE INTO {SILVER_DIM_SENSORS} t USING batch_dim_sens s ON t.device_id = s.device_id
        WHEN MATCHED THEN UPDATE SET t.zone = s.zone, t._last_updated = s._last_updated
        WHEN NOT MATCHED THEN INSERT *
    """)

    # --- 3. Insert Fact: Events ---
    fact_cols = [
        "id", "device_id", "event_timestamp", "zone",
        "air_quality_pm2_5", "air_quality_pm10", "air_quality_co",
        "air_quality_no2", "air_quality_o3", "air_quality_quality_index",
        "emergency_type", "emergency_severity", "emergency_response_time",
        "emergency_status", "traffic_vehicle_count", "traffic_avg_speed",
        "traffic_congestion_level", "traffic_road_type",
        "weather_temperature", "weather_humidity", "weather_wind_speed",
        "weather_condition", "_ingested_at", "data_quality_score",
        "has_air_quality_data", "has_traffic_data", "has_emergency_data", "has_weather_data"
    ]
    micro_batch_df.select(*fact_cols).write.format("iceberg").mode("append").save(SILVER_FACT_TABLE)


def read_bronze_stream(spark: SparkSession) -> DataFrame:
    logger.info("Opening streaming read from Bronze table: %s", BRONZE_TABLE)
    return spark.readStream.format("iceberg").option("stream-from-timestamp", "0").load(BRONZE_TABLE)


def write_silver_snowflake_stream(silver_df: DataFrame) -> None:
    logger.info("Starting Multi-Target Silver Snowflake Stream...")
    query = (
        silver_df.writeStream
        .foreachBatch(upsert_to_snowflake)
        .option("checkpointLocation", CHECKPOINT_DIR_SILVER)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received! Gracefully stopping Silver stream...")
        query.stop()
        logger.info("Silver stream stopped successfully.")

def run_silver_pipeline():
    spark = create_spark_session(app_name="SmartCity-Silver-Snowflake")
    
    logger.info("Ensuring Silver Snowflake tables exist...")
    spark.sql(DIM_LOCATIONS_DDL)
    spark.sql(DIM_SENSORS_DDL)
    spark.sql(FACT_EVENTS_DDL)

    bronze_stream_df = read_bronze_stream(spark)
    silver_df        = apply_silver_transformations(bronze_stream_df)
    write_silver_snowflake_stream(silver_df)


if __name__ == "__main__":
    run_silver_pipeline()
