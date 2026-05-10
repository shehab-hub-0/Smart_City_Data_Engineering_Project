# SmartCity Medallion Architecture Pipeline

> **Stack:** PySpark · Apache Iceberg · Project Nessie · MinIO · Kafka · Dremio

---

## Repository Structure

```
medallion_pipeline/
├── config/
│   ├── settings.py          # All env vars & constants (edit this first)
│   ├── schemas.py           # Kafka, Bronze, and Silver schema definitions
│   └── spark_session.py     # SparkSession builder with Iceberg + Nessie + MinIO
│
├── bronze/
│   └── bronze_ingestion.py  # Kafka → Iceberg (raw, uncleaned append stream)
│
├── silver/
│   └── silver_cleansing.py  # Bronze → Iceberg (typed, deduped, outlier-free)
│
├── gold/
│   └── gold_aggregations.py # Silver → 2 Gold tables (batch MERGE INTO or streaming)
│
├── utils/
│   ├── maintenance.py       # Iceberg compaction, snapshot expiry, orphan removal
│   └── data_quality.py      # DQ checks + pipeline status dashboard
│
└── submit.py                # spark-submit launcher
```

---

## Architecture Overview

```
Kafka Topics                      MinIO (s3a://lakehouse/)
smartcity.stream ──┐              ┌─────────────────────────────────────┐
                   ├─► Bronze ───►│ bronze_city_events  (raw strings)   │
smartcity.batch  ──┘  (stream)    │                                     │
                                  │ silver_city_events  (typed, clean)  │
                       Silver ───►│                                     │
                       (stream)   │ gold_hourly_zone_metrics            │
                                  │ gold_emergency_analysis             │
                       Gold    ──►│                                     │
                       (batch/    └─────────────────────────────────────┘
                        stream)            ▲
                                    Nessie Catalog
                                    (metadata + branches)
                                           ▲
                                        Dremio
                                    (BI / SQL queries)
```

---

## Quick Start

### 1. Prerequisites

| Service  | Version  | Notes                            |
|----------|----------|----------------------------------|
| PySpark  | 3.5.x    | Must match Iceberg runtime JAR   |
| Kafka    | 3.x      | Topics must exist before running |
| MinIO    | Latest   | Buckets: `lakehouse`, `spark-checkpoints` |
| Nessie   | 0.91.x   | Default branch: `main`           |
| Dremio   | 24.x     | Reads final Gold Iceberg tables  |

### 2. Create MinIO buckets

```bash
mc alias set local http://minio:9000 minioadmin minioadmin
mc mb local/lakehouse
mc mb local/spark-checkpoints
```

### 3. Configure environment (optional — defaults work for Docker Compose)

```bash
export KAFKA_BROKER="kafka1:19092"
export MINIO_ENDPOINT="http://minio:9000"
export NESSIE_URL="http://nessie:19120/api/v1"
export NESSIE_WAREHOUSE="s3a://lakehouse/warehouse"
```

### 4. Run each layer

```bash
# Terminal 1 — Bronze (runs forever, streaming)
python submit.py bronze

# Terminal 2 — Silver (runs forever, streaming)
python submit.py silver

# Terminal 3 — Gold (scheduled batch, runs once and exits)
python submit.py gold --mode batch

# Or Gold as a continuous stream
python submit.py gold --mode streaming

# Maintenance (run daily via cron/Airflow)
python submit.py maintenance

# Pipeline status dashboard
python submit.py status
```

---

## Data Quality Checks (Silver Layer)

| Check | Column | Threshold |
|-------|--------|-----------|
| Null rate | `id` | 0% |
| Null rate | `event_timestamp` | ≤ 1% |
| Null rate | `zone` | ≤ 5% |
| Duplicate rate | `id` | ≤ 0.01% |
| Temperature range | `weather_temperature` | [-50, 60] °C |
| Data freshness | `event_timestamp` | ≤ 30 minutes old |

---

## Data Imperfections & How They Are Fixed

| Imperfection | Real-World Scenario | Silver Fix |
|---|---|---|
| Sensor Dropout | `null` in `weather_temperature` | Fills categorical NULLs with `"unknown"`. Numeric NULLs preserved but heavily penalize the `data_quality_score`. |
| Format Noise | `08-05-2026 16:30` | `coalesce` attempts 8 different timestamp formats to parse safely into ISO. |
| Sensor Drift | `weather_temperature = 52.5°C` | **Clamping:** Bound strictly to `0–55°C`. Prevents downstream analytical anomalies. |
| Categorical Noise | `"  sunny  "` or `"RAINY"` | `trim()` and `lower()` standardizes all enumerations. |
| Duplicates | Re-emitted events with same `id` | `dropDuplicates(["id"])` within watermark window to ensure idempotent processing. |
| Late Data | Events arriving out of order | `withWatermark("event_timestamp", "10 minutes")` |

---

## Iceberg Table Properties

### Bronze
- Partition: `zone`, `days(_ingested_at)`
- Compression: ZSTD
- Target file size: 128 MB
- Snapshot retention: 7 days

### Silver
- Partition: `zone`, `days(event_timestamp)`
- Format version: **v2** (required for row-level deletes)
- Write modes: merge-on-read (efficient for streaming updates)
- Snapshot retention: 14 days

### Gold
- Partition: `zone`, `days(window_start)`
- Write modes: copy-on-write (fast reads, slower writes — OK for batch)
- Updated via `MERGE INTO` (upsert keyed on `zone + window_start`)

---

## Dremio Integration

After the Gold layer is populated, connect Dremio to Nessie:

1. In Dremio → **Sources** → **Add Source** → **Nessie**
2. Endpoint: `http://nessie:19120/api/v1`
3. Branch: `main`
4. AWS S3 credentials: `minioadmin` / `minioadmin`
5. Tables appear as `nessie.gold_hourly_zone_metrics`, etc.

Useful Dremio queries:

```sql
-- Hourly air quality trend by zone
SELECT zone, window_start, avg_pm2_5, avg_temperature_c
FROM   nessie.gold_hourly_zone_metrics
WHERE  window_start >= NOW() - INTERVAL '24' HOUR
ORDER  BY zone, window_start;

-- Top zones by emergency response time
SELECT zone, emergency_type, avg_response_time_sec, total_incidents
FROM   nessie.gold_emergency_analysis
ORDER  BY avg_response_time_sec DESC
LIMIT  20;
```

---

## Maintenance Schedule (Recommended)

| Job | Frequency | Command |
|-----|-----------|---------|
| Gold batch update | Every 15 min | `python submit.py gold --mode batch` |
| Data file compaction | Every 6 h | `python submit.py maintenance` |
| Snapshot expiry | Daily | Included in maintenance |
| Orphan file removal | Every 48 h | Included in maintenance |
| DQ report | Every 30 min | `python submit.py status` |

---

## JAR Dependencies (Maven)

```
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262
```

> ⚠️ All Spark / Scala / Iceberg versions must be compatible. The versions above
> are tested together. Do not mix Iceberg 1.5.x with Spark 3.4 JARs.
