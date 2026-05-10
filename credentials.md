# Smart City Data Platform — Credentials & Access Summary

This document contains all the login credentials and access URLs for the services running in this project.

## 🚀 Key Access Points

| Service | Local URL | Username / Email | Password / Token |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8090](http://localhost:8090) | `admin` | `admin` |
| **Conduktor Console** | [http://localhost:8080](http://localhost:8080) | (Setup on first visit) | - |
| **Spark Master** | [http://localhost:8085](http://localhost:8085) | - | - |
| **Spark Notebook** | [http://localhost:8889](http://localhost:8889) | - | `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` | `minioadmin` |
| **pgAdmin 4** | [http://localhost:5050](http://localhost:5050) | `admin@smartcity.com` | `admin` |
| **Nessie UI** | [http://localhost:19120](http://localhost:19120) | - | - |
| **Dremio UI** | [http://localhost:9047](http://localhost:9047) | (Setup on first visit) | - |

---

## 🛠️ Database Credentials

### Main PostgreSQL
- **Host**: `localhost:5432` (Internal: `postgres`)
- **Admin User**: `postgres` / `postgres`
- **Airflow DB**: `airflow` / `airflow`
- **Warehouse DB**: `smartcity` / `smartcity`

### Conduktor Metadata (Postgres)
- **Host**: `postgresql:5432`
- **User**: `conduktor`
- **Password**: `some_password`
- **Database**: `conduktor-console`

---

## 🔑 System Keys (Internal Use)
- **Airflow Fernet Key**: `pJy19Gl9mWSF1_muBOdYAOdmsyqwBWLjpaj9Y-zrwH4=`
- **Airflow JWT Secret**: `smart_city_platform_jwt_secret_32`
- **Kafka Cluster ID**: `MkU3OEVBNTcwNTJENDM2Qk`
