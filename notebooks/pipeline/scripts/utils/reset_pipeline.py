import logging
import os
import time
from p_config.spark_session import create_spark_session
from p_config.settings import (
    BRONZE_TABLE, SILVER_TABLE, SILVER_FACT_TABLE, SILVER_DIM_LOCATIONS, SILVER_DIM_SENSORS,
    GOLD_HOURLY_TABLE, GOLD_EMERGENCY_TABLE,
    CHECKPOINT_DIR_BRONZE, CHECKPOINT_DIR_SILVER, 
    CHECKPOINT_DIR_GOLD_HOURLY, CHECKPOINT_DIR_GOLD_EMERGENCY,
    NESSIE_WAREHOUSE, KAFKA_BROKER, KAFKA_TOPIC_STREAM, KAFKA_TOPIC_BATCH
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ResetPipeline")

def reset_environment():
    spark = create_spark_session("Medallion-Reset-Tool")
    sc = spark.sparkContext
    
    logger.info("🧨 Starting FULL RESET of Iceberg Tables & MinIO Storage...")
    
    # Order matters: Gold -> Silver -> Bronze (dependencies)
    tables = [
        GOLD_EMERGENCY_TABLE, GOLD_HOURLY_TABLE, 
        SILVER_FACT_TABLE, SILVER_DIM_LOCATIONS, SILVER_DIM_SENSORS,
        SILVER_TABLE, BRONZE_TABLE
    ]
    
    paths_to_delete = [
        CHECKPOINT_DIR_BRONZE, CHECKPOINT_DIR_SILVER,
        CHECKPOINT_DIR_GOLD_HOURLY, CHECKPOINT_DIR_GOLD_EMERGENCY,
        NESSIE_WAREHOUSE
    ]

    # --- Step A: Drop Iceberg Tables with PURGE ---
    logger.info("🧨 Attempting to DROP and PURGE tables...")
    for table in tables:
        logger.info(f"🗑️ Dropping table with PURGE: {table}")
        try:
            # PURGE is critical to tell Iceberg to delete physical files
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
            logger.info(f"✅ Table {table} purged from catalog.")
        except Exception as e:
            logger.warning(f"⚠️ Failed to PURGE {table} (might already be gone or corrupt): {e}")
            try:
                logger.info(f"🔄 Falling back to standard DROP for {table}...")
                spark.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception as e2:
                logger.error(f"❌ Failed to drop {table} even with fallback: {e2}")

    # --- Step B: Delete Physical Files (Checkpoints & Warehouse) ---
    logger.info("🧹 Purging physical files from MinIO (Checkpoints & Warehouse)...")
    try:
        hadoop_conf = sc._jsc.hadoopConfiguration()
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

        for path_str in paths_to_delete:
            if not path_str.endswith("/"):
                path_str += "/"
                
            logger.info(f"📂 Processing directory: {path_str}")
            try:
                fs_path = Path(path_str)
                fs = FileSystem.get(fs_path.toUri(), hadoop_conf)
                
                if fs.exists(fs_path):
                    if path_str == NESSIE_WAREHOUSE or path_str == f"{NESSIE_WAREHOUSE}/":
                        logger.info(f"🌊 Cleaning children of warehouse root: {path_str}")
                        # List all files/folders in root and delete them
                        status = fs.listStatus(fs_path)
                        if status:
                            for s in status:
                                child_path = s.getPath()
                                logger.info(f"  🗑️ Deleting child: {child_path}")
                                fs.delete(child_path, True)
                        logger.info(f"✅ Warehouse children cleaned.")
                    else:
                        logger.info(f"🌊 Deleting: {path_str}")
                        fs.delete(fs_path, True) # True = Recursive
                        logger.info(f"✅ Deleted {path_str}")
                else:
                    logger.info(f"ℹ️ Path {path_str} does not exist, skipping.")
            except Exception as e:
                logger.error(f"❌ Failed to delete {path_str}: {e}")

    except Exception as e:
        logger.error(f"❌ critical error during file purging: {e}")

    logger.info("✨ Reset complete! You are now at a clean-slate state.")

if __name__ == "__main__":
    reset_environment()
