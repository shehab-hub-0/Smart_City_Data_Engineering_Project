"""
Smart City Data Generation – Configuration
"""

import os

# ── Reproducibility ──────────────────────────────────────────
SEED = 42

# ── Generation settings ─────────────────────────────────────
NUM_RECORDS = 30  # records per domain per run
BATCH_SIZE = 50  # DB / Kafka batch size

# ── City zones with representative lat/lon centers ───────────
CITY_ZONES = {
    "downtown": {"lat": 30.0444, "lon": 31.2357, "radius": 0.01},
    "residential": {"lat": 30.0600, "lon": 31.2200, "radius": 0.015},
    "industrial": {"lat": 30.0200, "lon": 31.2600, "radius": 0.02},
    "commercial": {"lat": 30.0500, "lon": 31.2500, "radius": 0.012},
    "suburban": {"lat": 30.0800, "lon": 31.2000, "radius": 0.025},
}

# ── Data-quality knobs ───────────────────────────────────────
MISSING_RATE = 0.07  # 7 % null injection
OUTLIER_RATE = 0.03  # 3 % extreme values
WRONG_TYPE_RATE = 0.02  # 2 % type corruption
DUPLICATE_RATE = 0.04  # 4 % duplicate rows
TIMESTAMP_JITTER_SECONDS = 30  # max seconds of disorder

# ── CSV output ───────────────────────────────────────────────
CSV_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "csv"
)

# ── PostgreSQL ───────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DATABASE", "smartcity"),
    "user": os.getenv("PG_USER", "smartcity"),
    "password": os.getenv("PG_PASSWORD", "smartcity"),
}

# ── MinIO ──────────────────────────────────────────────────
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "secure": False,
}
MINIO_BUCKET = "landing-zone"

# ── Kafka ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092")
KAFKA_TOPICS = {
    "air_quality": "smartcity.air_quality",
    "emergency": "smartcity.emergency",
    "traffic": "smartcity.traffic",
    "weather": "smartcity.weather",
    "unified": "smartcity.unified",
}
KAFKA_SEND_DELAY = 1.0  # seconds between messages (simulate real-time)

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-22s | %(message)s"
