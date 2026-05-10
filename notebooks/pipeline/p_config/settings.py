"""
================================================================================
 Medallion Pipeline — Central Configuration & Constants
================================================================================
 All environment-specific values live here. In production, replace hard-coded
 secrets with environment variables or a secrets manager (Vault, AWS SM, etc.)
================================================================================
"""

import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER          = os.getenv("KAFKA_BROKER",          "kafka1:19092")
KAFKA_TOPIC_STREAM    = os.getenv("KAFKA_TOPIC_STREAM",    "smartcity.stream")
KAFKA_TOPIC_BATCH     = os.getenv("KAFKA_TOPIC_BATCH",     "smartcity.batch")
# Comma-separated list consumed by the bronze reader
KAFKA_TOPICS          = f"{KAFKA_TOPIC_STREAM},{KAFKA_TOPIC_BATCH}"

# ── MinIO / S3-compatible object store ────────────────────────────────────────
MINIO_ENDPOINT        = os.getenv("MINIO_ENDPOINT",        "http://minio:9000")
MINIO_ACCESS_KEY      = os.getenv("MINIO_ACCESS_KEY",      "minioadmin")
MINIO_SECRET_KEY      = os.getenv("MINIO_SECRET_KEY",      "minioadmin")

# ── Project Nessie Catalog ────────────────────────────────────────────────────
NESSIE_URL            = os.getenv("NESSIE_URL",            "http://nessie:19120/api/v1")
NESSIE_DEFAULT_BRANCH = os.getenv("NESSIE_DEFAULT_BRANCH", "main")
# Iceberg warehouse root — all table data lives under this s3a:// prefix
NESSIE_WAREHOUSE      = os.getenv("NESSIE_WAREHOUSE",      "s3a://warehouse")

# ── Checkpoint directories (stored in MinIO) ──────────────────────────────────
CHECKPOINT_DIR_BRONZE = os.getenv(
    "CHECKPOINT_DIR_BRONZE", "s3a://spark-checkpoints/bronze_pure_kafka"
)
CHECKPOINT_DIR_SILVER = os.getenv(
    "CHECKPOINT_DIR_SILVER", "s3a://spark-checkpoints/silver_clean"
)
CHECKPOINT_DIR_GOLD_HOURLY = os.getenv(
    "CHECKPOINT_DIR_GOLD_HOURLY", "s3a://spark-checkpoints/gold_hourly"
)
CHECKPOINT_DIR_GOLD_EMERGENCY = os.getenv(
    "CHECKPOINT_DIR_GOLD_EMERGENCY", "s3a://spark-checkpoints/gold_emergency"
)

# ── Iceberg Table Names (fully qualified: catalog.table) ──────────────────────
BRONZE_TABLE          = "nessie.bronze_city_events"
SILVER_TABLE          = "nessie.silver_fact_city_events"  # Main fact table
SILVER_FACT_TABLE     = "nessie.silver_fact_city_events"
SILVER_DIM_LOCATIONS  = "nessie.silver_dim_locations"
SILVER_DIM_SENSORS    = "nessie.silver_dim_sensors"
GOLD_HOURLY_TABLE     = "nessie.gold_hourly_zone_metrics"
GOLD_EMERGENCY_TABLE  = "nessie.gold_emergency_analysis"

# ── Watermark & windowing settings ────────────────────────────────────────────
# Maximum tolerated event lateness before Spark drops the record from state.
WATERMARK_DELAY       = "10 minutes"
# Width of the tumbling window used in Gold hourly aggregations.
GOLD_WINDOW_DURATION  = "1 hour"

# ── Outlier thresholds ────────────────────────────────────────────────────────
# Values outside these physical bounds are replaced with NULL in Silver.
TEMP_MIN_C            = -50.0    # °C — absolute cold limit (below Antarctic record)
TEMP_MAX_C            =  60.0    # °C — above 60 °C is physically implausible outdoors
PM25_MAX              = 1000.0   # µg/m³
PM10_MAX              = 2000.0   # µg/m³
CO_MAX                = 50.0     # ppm (lethal at ~1200 ppm; sensor saturation ~50)
NO2_MAX               = 10.0     # ppm
O3_MAX                = 1.0      # ppm
VEHICLE_COUNT_MAX     = 10_000   # vehicles per reading — >10k is sensor malfunction
AVG_SPEED_MIN         = 0.0
AVG_SPEED_MAX         = 300.0    # km/h

# ── Streaming micro-batch trigger interval ────────────────────────────────────
TRIGGER_INTERVAL      = "30 seconds"   # set "0 seconds" for fastest possible
