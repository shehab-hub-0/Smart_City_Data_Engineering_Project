import os
import pandas as pd

try:
    from producers.config import CSV_OUTPUT_DIR
except ImportError:
    from config import CSV_OUTPUT_DIR


class CSVWriter:
    def __init__(self):
        self.output_dir = CSV_OUTPUT_DIR
        os.makedirs(self.output_dir, exist_ok=True)

    def write(self, domain, dataframe):
        """
        Writes data to CSV, partitioned by current date.
        """
        # Get date from first record's timestamp or current date
        if not dataframe.empty:
            sample_ts = dataframe["timestamp"].iloc[0]
            # Ensure sample_ts is datetime
            if isinstance(sample_ts, str):
                try:
                    sample_ts = pd.to_datetime(sample_ts)
                except Exception as e:
                    print(f"Error writing to CSV: {e}")
                    sample_ts = pd.Timestamp.now()
            date_str = sample_ts.strftime("%Y-%m-%d")
        else:
            date_str = pd.Timestamp.now().strftime("%Y-%m-%d")

        domain_dir = os.path.join(self.output_dir, domain, f"date={date_str}")
        os.makedirs(domain_dir, exist_ok=True)

        file_path = os.path.join(
            domain_dir, f"{domain}_{pd.Timestamp.now().strftime('%H%M%S')}.csv"
        )

        dataframe.to_csv(file_path, index=False)
        print(f"✅ Saved {len(dataframe)} records to {file_path}")
