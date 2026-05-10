# 🚧 Challenges & Solutions

Building a production-ready Data Lakehouse involves overcoming several architectural and operational hurdles. Below are the key challenges faced during the development of this Smart City platform and their implemented solutions:

---

### 1. CI/CD & Linting Automation
**Problem:** Frequent pipeline failures in GitHub Actions due to stylistic and non-critical formatting errors (e.g., missing Docstrings, bare excepts).

**Solution:** Refactored the codebase to adhere to PEP8 standards. Fine-tuned the `.github/workflows/ci.yml` pipeline to focus on logical errors and syntax validation specifically tailored for Airflow 3.1.2, achieving a stable and reliable automated CI process.

---

### 2. Observability Integration (cAdvisor & WSL2)
**Problem:** Integrating cAdvisor with Prometheus on a Windows/WSL2 host environment caused pathing errors when attempting to access CPU metric directories.

**Solution:** Modified the `docker-compose.yml` to adjust volume bindings and applied strict ignore rules to exclude WSL2 specific system files, allowing continuous performance tracking without system conflicts.

---

### 3. Secrets Management & Security
**Problem:** Hardcoded sensitive credentials (SMTP emails, database passwords) within Python scripts and configuration YAMLs posed a significant security risk for version control.

**Solution:** Conducted a comprehensive security sweep. Migrated all credentials to an isolated `.env` file (explicitly added to `.gitignore`). Utilized dynamic environment variable injection (`${VARIABLE}`) inside Docker Compose.

---

### 4. Lakehouse Stability (Nessie & Iceberg)
**Problem:** Encountered `NotFoundException` errors caused by metadata inconsistency when Parquet files were manually deleted from MinIO, while Project Nessie still held active pointers.

**Solution:** Enforced strict data management protocols using Iceberg SQL commands (`DROP TABLE nessie.<table> PURGE`). Leveraged Nessie's Git-like branching capabilities to isolate pipeline testing in a `dev` branch before merging to `main`, preventing accidental data corruption in production.

---

### 5. Alerting as Code
**Problem:** Manual alert configurations in Grafana were ephemeral and lost every time the monitoring containers were torn down or recreated.

**Solution:** Implemented **"Alerting as Code"** by defining alert rules and notification policies systematically in `alerts.yaml`. This guarantees that critical threshold monitors (e.g., CPU load, Container Down) are automatically provisioned at startup.

---

[⬅️ Back to Index](./README.md)
