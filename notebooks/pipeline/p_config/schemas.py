"""
================================================================================
 Medallion Pipeline — Schema Definitions
================================================================================
 Three schema variants:
   1. KAFKA_RAW_SCHEMA   — the JSON envelope arriving from Kafka
   2. BRONZE_SCHEMA      — all-string version stored as-is in Bronze
   3. SILVER_SCHEMA      — fully typed, cleaned schema for Silver and above
================================================================================
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
)

# ── 1. Kafka JSON envelope schema ─────────────────────────────────────────────
# The Kafka message VALUE is a JSON string. The actual sensor payload lives
# inside the `processedValue` field (also JSON-encoded string), or alternatively
# under `value.data`.  We parse the outer envelope first, then explode the inner
# payload in the Bronze step.
KAFKA_ENVELOPE_SCHEMA = StructType([
    StructField("processedValue", StringType(), True),   # inner JSON string
    StructField("value",          StringType(), True),   # fallback path
    StructField("key",            StringType(), True),
    StructField("topic",          StringType(), True),
    StructField("partition",      IntegerType(), True),
    StructField("offset",         StringType(), True),
    StructField("timestamp",      StringType(), True),   # Kafka ingestion ts
])

# ── 2. Inner sensor-data schema (all Strings from Kafka) ─────────────────────
# Kafka producers emit every field as a string. We store them verbatim in
# Bronze and only cast to proper types in Silver.
SENSOR_PAYLOAD_SCHEMA = StructType([
    # ── Identity & location ──────────────────────────────────────────────────
    StructField("id",                         StringType(), True),
    StructField("timestamp",                  StringType(), True),   # ISO-8601
    StructField("zone",                       StringType(), True),
    StructField("device_id",                  StringType(), True),
    StructField("latitude",                   StringType(), True),
    StructField("longitude",                  StringType(), True),
    StructField("metadata_source_type",       StringType(), True),

    # ── Air quality ──────────────────────────────────────────────────────────
    StructField("air_quality_pm2_5",          StringType(), True),   # may contain "_err"
    StructField("air_quality_pm10",           StringType(), True),
    StructField("air_quality_co",             StringType(), True),
    StructField("air_quality_no2",            StringType(), True),
    StructField("air_quality_o3",             StringType(), True),
    StructField("air_quality_quality_index",  StringType(), True),

    # ── Emergency ────────────────────────────────────────────────────────────
    StructField("emergency_type",             StringType(), True),
    StructField("emergency_severity",         StringType(), True),
    StructField("emergency_response_time",    StringType(), True),
    StructField("emergency_status",           StringType(), True),

    # ── Traffic ──────────────────────────────────────────────────────────────
    StructField("traffic_vehicle_count",      StringType(), True),
    StructField("traffic_avg_speed",          StringType(), True),
    StructField("traffic_congestion_level",   StringType(), True),
    StructField("traffic_road_type",          StringType(), True),

    # ── Weather ──────────────────────────────────────────────────────────────
    StructField("weather_temperature",        StringType(), True),
    StructField("weather_humidity",           StringType(), True),
    StructField("weather_wind_speed",         StringType(), True),
    StructField("weather_condition",          StringType(), True),

    # ── Lineage ──────────────────────────────────────────────────────────────
    StructField("_source_format",             StringType(), True),
])

# ── 3. Silver (fully typed) schema ───────────────────────────────────────────
# After cleansing, every numeric field is cast to its canonical type.
SILVER_SCHEMA = StructType([
    StructField("id",                         StringType(),    False),  # NOT NULL
    StructField("event_timestamp",            TimestampType(), True),   # parsed ISO-8601
    StructField("zone",                       StringType(),    True),
    StructField("device_id",                  StringType(),    True),
    StructField("latitude",                   DoubleType(),    True),
    StructField("longitude",                  DoubleType(),    True),
    StructField("metadata_source_type",       StringType(),    True),

    StructField("air_quality_pm2_5",          DoubleType(),    True),
    StructField("air_quality_pm10",           DoubleType(),    True),
    StructField("air_quality_co",             DoubleType(),    True),
    StructField("air_quality_no2",            DoubleType(),    True),
    StructField("air_quality_o3",             DoubleType(),    True),
    StructField("air_quality_quality_index",  IntegerType(),   True),

    StructField("emergency_type",             StringType(),    True),
    StructField("emergency_severity",         StringType(),    True),
    StructField("emergency_response_time",    DoubleType(),    True),   # seconds
    StructField("emergency_status",           StringType(),    True),

    StructField("traffic_vehicle_count",      IntegerType(),   True),
    StructField("traffic_avg_speed",          DoubleType(),    True),
    StructField("traffic_congestion_level",   StringType(),    True),
    StructField("traffic_road_type",          StringType(),    True),

    StructField("weather_temperature",        DoubleType(),    True),
    StructField("weather_humidity",           DoubleType(),    True),
    StructField("weather_wind_speed",         DoubleType(),    True),
    StructField("weather_condition",          StringType(),    True),

    StructField("_source_format",             StringType(),    True),
    StructField("_ingested_at",               TimestampType(), True),   # Bronze write time
    StructField("_silver_processed_at",       TimestampType(), True),   # Silver write time
])
