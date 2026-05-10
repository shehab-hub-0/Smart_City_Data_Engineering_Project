# 🚀 Setup & Troubleshooting Guide

This guide helps you launch the project from scratch and resolve common issues.

---

## 🛠️ Initial Setup Steps

1.  **Build and Start Containers:**
    ```bash
    docker-compose up -d --build
    ```
2.  **Verify Service Health:**
    Wait until all services show as `healthy` in Docker Desktop or via `docker ps`.
3.  **Prepare MinIO Buckets:**
    Access `http://localhost:9001` and create buckets: `warehouse` and `spark-checkpoints`.
4.  **Create Nessie Branch:**
    ```bash
    python -c "from p_config.spark_session import create_spark_session; spark = create_spark_session(); spark.sql('CREATE BRANCH IF NOT EXISTS main IN nessie'); print('✅ Branch main created')"
    ```
5.  **Configure Email Alerts:**
    -   Open the `.env` file in the project root.
    -   Provide your SMTP credentials (e.g., Gmail App Password).
    -   Restart Grafana: `docker-compose up -d grafana`.
    -   Verify in Grafana UI: **Alerting** > **Contact points** > **Test**.

---

## 🔍 Common Troubleshooting

### 1. `Metadata not found` (NotFoundException)
**Cause:** Manual deletion of files in MinIO while Nessie still has pointers to them.
**Solution:**
-   Force drop the tables using `DROP TABLE nessie.<table> PURGE`.
-   Or switch to a clean branch (e.g., `prod`) by updating `settings.py`.

### 2. `Falling Behind` in Spark Streaming
**Cause:** Inbound data rate is higher than processing throughput (common during backfill).
**Solution:** This is normal during initial ingestion. The system will stabilize once it catches up with the backlog.

### 3. `DateTimeParseException`
**Cause:** Timestamp format mismatch between Kafka events and Spark 3.x expectations.
**Solution:** Ensure the following config is present in `spark_session.py`:
```python
.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
```

### 4. Reset Everything
To wipe all data and start completely fresh:
1.  Stop containers: `docker-compose down -v` (The `-v` flag deletes all volumes/data).
2.  Delete any `__pycache__` folders and local checkpoint directories.
3.  Restart from Step 1.

---

## 📞 Monitoring URLs
-   **Kafka UI:** `http://localhost:8080` (Sensor monitoring).
-   **Spark UI:** `http://localhost:4040` (Processing details).
-   **Airflow:** `http://localhost:8090` (Orchestration).
-   **Grafana:** `http://localhost:3000` (Dashboards).
-   **Prometheus:** `http://localhost:9090` (Metrics).
-   **cAdvisor:** `http://localhost:8087` (Container Stats).

---

> [!NOTE]
> Next Step: Read about the technical hurdles we overcame in **[🚧 Challenges & Solutions](./07_Challenges_and_Solutions.md)**.

[⬅️ Back to Index](./README.md)
