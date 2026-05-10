-- Airflow Database and User
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER DATABASE airflow OWNER TO airflow;

-- Smart City Data Warehouse Database and User
CREATE DATABASE smartcity;
CREATE USER smartcity WITH ENCRYPTED PASSWORD 'smartcity';
GRANT ALL PRIVILEGES ON DATABASE smartcity TO smartcity;
ALTER DATABASE smartcity OWNER TO smartcity;

\c smartcity;

-- Schemas for Medallion Architecture / Data Warehouse
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION smartcity;
CREATE SCHEMA IF NOT EXISTS warehouse AUTHORIZATION smartcity;
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION smartcity;

-- Basic tables for Analytics (Created by Spark jobs usually, but good to ensure they exist or are ready)
-- We will let Spark create tables automatically via JDBC.
