import os
import logging
from p_config.spark_session import create_spark_session
from p_config.settings import BRONZE_TABLE, SILVER_TABLE

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def export_gold_tables():
    """Reads Gold tables from Nessie and exports them as single CSV files."""
    spark = create_spark_session(app_name="Gold-To-CSV-Exporter")
    
    # Define local export path (mounted to host)
    export_base_path = "/home/jovyan/work/pipeline/exports"
    os.makedirs(export_base_path, exist_ok=True)

    tables_to_export = [
        {"name": BRONZE_TABLE,    "filename": "bronze_city_events.csv"},
        {"name": SILVER_TABLE, "filename": "silver_city_events.csv"}
    ]

    print("\n" + "="*60)
    print("🚀 EXPORTING GOLD TABLES TO CSV")
    print("="*60 + "\n")

    try:
        for item in tables_to_export:
            table_full_name = item["name"]
            target_file = os.path.join(export_base_path, item["filename"])
            
            print(f"📦 Reading {table_full_name}...")
            
            # Read from Iceberg
            df = spark.table(table_full_name)
            
            row_count = df.count()
            if row_count == 0:
                print(f"⚠️ Table {table_full_name} is empty. Skipping.")
                continue

            print(f"💾 Saving {row_count} rows to {target_file}...")
            
            # Coalesce to 1 to get a single CSV file, overwrite if exists
            # We use header=True to include column names
            df.coalesce(1).write.mode("overwrite").option("header", "True").csv(target_file + "_tmp")
            
            # Spark saves as a folder, let's find the actual CSV inside and move it
            tmp_folder = target_file + "_tmp"
            actual_csv = [f for f in os.listdir(tmp_folder) if f.endswith(".csv")][0]
            
            # Move and rename
            os.replace(os.path.join(tmp_folder, actual_csv), target_file)
            
            # Clean up the tmp folder
            import shutil
            shutil.rmtree(tmp_folder)
            
            print(f"✅ Successfully exported: {item['filename']}")
            print("-" * 40)

    except Exception as e:
        print(f"❌ Export failed: {e}")
    finally:
        spark.stop()
        print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    export_gold_tables()
