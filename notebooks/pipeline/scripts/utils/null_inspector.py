import logging
from pyspark.sql import functions as F
from p_config.spark_session import create_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def inspect_nulls():
    """Analyzes all tables in Nessie and reports null counts for every column."""
    spark = create_spark_session(app_name="Null-Inspector")
    
    print("\n" + "="*80)
    print("📊 DATA QUALITY: NULL VALUES INSPECTOR")
    print("="*80 + "\n")

    try:
        # Get all tables
        tables = spark.sql("SHOW TABLES IN nessie").collect()

        for t_row in tables:
            table_name = t_row['tableName']
            full_name = f"nessie.{table_name}"
            
            print(f"🔍 Analyzing Table: {full_name}...")
            
            df = spark.table(full_name)
            total_rows = df.count()
            
            if total_rows == 0:
                print(f"⚠️ Table is EMPTY. Skipping analysis.")
                print("-" * 40)
                continue

            print(f"📈 Total Rows: {total_rows}")
            
            # Create a list of aggregation expressions for each column
            null_counts = []
            for col_name in df.columns:
                null_counts.append(
                    F.count(F.when(F.col(col_name).isNull(), 1)).alias(col_name)
                )
            
            # Run the aggregation
            result = df.select(null_counts).collect()[0].asDict()
            
            # Print report for columns that HAVE nulls
            has_nulls = False
            print(f"\n{'Column Name':<30} | {'Null Count':<10} | {'Null %':<10}")
            print("-" * 55)
            
            for col_name, count in result.items():
                if count > 0:
                    percentage = (count / total_rows) * 100
                    print(f"{col_name:<30} | {count:<10} | {percentage:.2f}%")
                    has_nulls = True
            
            if not has_nulls:
                print("✨ No NULL values found in any column! (Perfect Quality)")
                
            print("\n" + "-" * 40 + "\n")

    except Exception as e:
        print(f"❌ Error inspecting nulls: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    inspect_nulls()
