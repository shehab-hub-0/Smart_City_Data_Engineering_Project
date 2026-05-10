import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

try:
    from config import DB_CONFIG
except ImportError:
    from data.config import DB_CONFIG


class DBWriter:
    def __init__(self):
        self.config = DB_CONFIG
        self.conn = None
        self.cursor = None

    def _connect(self):
        try:
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"❌ DB Connection failed: {e}")
            raise

    def _create_table_if_not_exists(self, table_name, columns):
        """
        Dynamically creates target table based on column names.
        Maps simple python types to PG types.
        """
        # All columns as TEXT to allow for intentional dirty data (e.g., strings in numeric columns)
        # Spark will handle the cleaning and type casting later.
        col_defs = [f"{col} TEXT" for col in columns]

        create_sql = (
            f"CREATE TABLE IF NOT EXISTS public.{table_name} ({', '.join(col_defs)});"
        )
        self.cursor.execute(create_sql)
        self.conn.commit()

    def write(self, domain, dataframe):
        if self.conn is None:
            self._connect()

        table_name = f"raw_{domain}"
        cols = list(dataframe.columns)
        self._create_table_if_not_exists(table_name, cols)

        # Convert df to list of tuples for psycopg2
        # Replace NaN with None for PG nulls
        records = dataframe.where(pd.notnull(dataframe), None).values.tolist()

        query = f"INSERT INTO public.{table_name} ({', '.join(cols)}) VALUES %s"

        try:
            execute_values(self.cursor, query, records)
            self.conn.commit()
            print(f"✅ inserted {len(records)} records into table {table_name}")
        except Exception as e:
            self.conn.rollback()
            print(f"❌ DB Insert failed for {table_name}: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
