"""
================================================================================
 Medallion Pipeline — spark-submit launcher script
================================================================================
 Run each layer independently:
   python submit.py bronze
   python submit.py silver
   python submit.py gold --mode batch
   python submit.py gold --mode streaming
   python submit.py maintenance
   python submit.py status
================================================================================
"""

import subprocess
import sys
import os

# Resolve project root (directory this file lives in)
ROOT = os.path.dirname(os.path.abspath(__file__))

# ── JAR packages (Maven coordinates) ─────────────────────────────────────────
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

BASE_SUBMIT = [
    "spark-submit",
    "--master", "local[*]",           # replace with spark://... for cluster
    "--packages", PACKAGES,
    "--conf", "spark.driver.memory=512m",
    "--conf", "spark.sql.shuffle.partitions=2",
    "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
    # Include the project root on the Python path so config.* imports work
    "--py-files", ROOT,
]

SCRIPTS = {
    "bronze":      os.path.join(ROOT, "bronze", "bronze_ingestion.py"),
    "silver":      os.path.join(ROOT, "silver", "silver_cleansing.py"),
    "gold":        os.path.join(ROOT, "gold",   "gold_aggregations.py"),
    "maintenance": os.path.join(ROOT, "utils",  "maintenance.py"),
    "status":      os.path.join(ROOT, "utils",  "data_quality.py"),
}


def submit(layer: str, extra_args: list = None):
    script = SCRIPTS.get(layer)
    if not script:
        print(f"Unknown layer: {layer}. Choose from {list(SCRIPTS.keys())}")
        sys.exit(1)

    cmd = BASE_SUBMIT + [script] + (extra_args or [])
    print(f"Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python submit.py <layer> [--mode batch|streaming]")
        sys.exit(1)

    layer_arg  = sys.argv[1]
    extra      = sys.argv[2:]   # pass-through extra args to the script
    submit(layer_arg, extra)
