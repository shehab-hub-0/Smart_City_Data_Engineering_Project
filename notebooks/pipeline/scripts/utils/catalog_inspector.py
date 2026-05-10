import logging
from p_config.spark_session import create_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def inspect_catalog():
    """Iterates through all tables in Nessie and prints their physical storage locations."""
    spark = create_spark_session(app_name="Catalog-Inspector")
    
    print("\n" + "="*80)
    print("🔍 ICEBERG CATALOG INSPECTOR (NESSIE)")
    print("="*80 + "\n")

    try:
        # 1. Get list of all tables in the nessie catalog
        tables_df = spark.sql("SHOW TABLES IN nessie")
        tables = tables_df.collect()

        if not tables:
            print("📭 No tables found in the catalog.")
            return

        print(f"✅ Found {len(tables)} table(s):\n")
        
        # 2. Iterate through each table and get its location
        for row in tables:
            table_name = row['tableName']
            full_name = f"nessie.{table_name}"
            
            # Use DESCRIBE TABLE EXTENDED to get metadata
            details_df = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}")
            
            # Filter the location row
            location_row = details_df.filter("col_name = 'Location'").collect()
            
            if location_row:
                location = location_row[0]['data_type']
                print(f"📌 Table: {full_name}")
                print(f"   📂 Location: {location}")
                print("-" * 40)
            else:
                print(f"📌 Table: {full_name} (Location not found)")

    except Exception as e:
        print(f"❌ Error inspecting catalog: {e}")
    finally:
        print("\n" + "="*80 + "\n")
        spark.stop()

if __name__ == "__main__":
    inspect_catalog()
