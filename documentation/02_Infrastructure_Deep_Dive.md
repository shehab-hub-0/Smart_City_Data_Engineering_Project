# 🖥️ Infrastructure Deep Dive

The project is fully containerized using **Docker** to ensure consistency across environments. The platform consists of 15 services working in harmony.

---

## 🏗️ Core Services

### 1. Storage & Database Layer
-   **Postgres (Port 5432):**
    -   Serves as the metadata store for Airflow and Superset.
    -   Hosts the `airflow` database for task management.
-   **MinIO (Ports 9005, 9001):**
    -   The "Data Lake". All Parquet files and Iceberg metadata are stored here.
    -   Provides an S3-compatible interface.
-   **Redis (Port 6379):**
    -   Acts as the message broker for Airflow's Celery Executor to distribute tasks to workers.

### 2. Ingestion Layer (Real-time)
-   **Kafka (Port 9092):**
    -   The engine that receives sensor data. Runs in a KRaft-enabled mini-cluster.
-   **Conduktor Console (Port 8080):**
    -   A professional UI to monitor Kafka topics, inspect messages, and manage the cluster.

### 3. Processing Layer (Spark)
-   **Spark Master (Port 8085):**
    -   The coordinator that distributes processing tasks to workers.
-   **Spark Worker:**
    -   The actual processing engine that executes Python/Spark code.
-   **Spark Notebook (Port 8889):**
    -   Jupyter Lab environment for interactive pipeline development. Default token: `admin`.

### 4. Catalog & Analytics Layer
-   **Nessie (Port 19120):**
    -   The "Git for Data". Allows for commits and branching on Iceberg tables to prevent data loss and allow experimentation.
-   **Dremio (Port 9047):**
    -   SQL analytics engine. Connects to Nessie and allows analysts to query the Data Lake directly using SQL.

### 5. Orchestration Layer (Airflow)
-   **Airflow Webserver (Port 8090):**
    -   The UI where you monitor DAGs and pipeline execution status.
-   **Airflow Scheduler/Worker/Triggerer:**
    -   Backend engines ensuring tasks are executed according to their schedules.

### 6. Observability Layer
-   **Prometheus (Port 9090):**
    -   Collects time-series metrics from all containers and the host system.
-   **cAdvisor (Port 8087):**
    -   Provides real-time resource usage (CPU, RAM, Network) for every running Docker container.
-   **Grafana (Port 3000):**
    -   The visualization hub. Pre-configured with two major dashboards:
        1. **Executive Dashboard:** Real-time city metrics from Dremio.
        2. **System Health:** Container and infrastructure performance metrics.

---

## 🚪 Port Cheat Sheet

| Service | Host Port | URL |
| :--- | :--- | :--- |
| **Spark UI (Notebook)** | 8889 | `http://localhost:8889` |
| **Airflow UI** | 8090 | `http://localhost:8090` |
| **MinIO Console** | 9001 | `http://localhost:9001` |
| **Dremio UI** | 9047 | `http://localhost:9047` |
| **Grafana UI** | 3000 | `http://localhost:3000` |
| **Prometheus UI** | 9090 | `http://localhost:9090` |
| **cAdvisor UI** | 8087 | `http://localhost:8087` |
| **Conduktor Console** | 8080 | `http://localhost:8080` |
| **pgAdmin** | 5050 | `http://localhost:5050` |

---

## 🔒 Security & Secrets Management

The platform adheres to strict security standards for secret management:

-   **Zero Hardcoding:** No passwords, API keys, or emails are stored in the source code or YAML files.
-   **Centralized `.env`:** All sensitive credentials (DB passwords, MinIO keys, Dremio tokens) are stored in a centralized `.env` file.
-   **Git Security:** The `.env` file is explicitly ignored in `.gitignore` to prevent accidental exposure on version control systems.
-   **Dynamic Injection:** Docker Compose dynamically injects these secrets into the containers at runtime using the `${VARIABLE}` syntax.

---

## 🔔 Alerting & Incident Management

The platform includes a pre-configured, GitOps-based alerting system to ensure 24/7 reliability:

-   **Proactive Monitoring:** Alert rules are defined in `config/grafana/provisioning/alerting/alerts.yaml`.
-   **Core Alarms:**
    -   **Service Down:** Instant "Critical" alert if any container stops.
    -   **Resource Stress:** "Warning" alerts if CPU exceeds 85% or RAM exceeds 90% for sustained periods.
-   **Automated Notifications:** Alerts are automatically routed to the platform administrators via **Email** (SMTP).
-   **Intelligent Routing:** Critical alerts trigger faster notifications and can be routed to different "Contact Points" than warnings.

---

> [!NOTE]
> Next Step: Explore the **[💎 Data Processing Layers](./03_Data_Processing_Layers.md)** and the Medallion pipeline.

[⬅️ Back to Index](./README.md)
