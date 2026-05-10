"""
================================================================================
 Medallion Pipeline — Data Quality Monitor & Orchestration Utilities
================================================================================
 Provides:
   1. DataQualityMonitor — runs DQ checks on Silver and logs results
   2. run_pipeline_status() — prints a snapshot of all table row counts
   3. verify_nessie_catalog() — confirms Nessie connectivity and lists tables
================================================================================
"""

import logging
from dataclasses import dataclass, field
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from p_config.spark_session import create_spark_session
from p_config.settings import BRONZE_TABLE, SILVER_TABLE, GOLD_HOURLY_TABLE, GOLD_EMERGENCY_TABLE

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class DQResult:
    check_name:   str
    table:        str
    passed:       bool
    value:        float
    threshold:    float
    message:      str = ""


@dataclass
class DataQualityMonitor:
    """
    Runs a suite of data quality checks against the Silver (or any) table.
    Results are logged and can be pushed to an external monitoring system.
    """
    spark:   SparkSession
    table:   str
    results: List[DQResult] = field(default_factory=list)

    def _load(self) -> DataFrame:
        return self.spark.read.format("iceberg").load(self.table)

    def check_null_rate(self, column: str, max_null_pct: float = 30.0) -> DQResult:
        """Fail if more than `max_null_pct` % of rows have NULL in `column`."""
        df = self._load()
        total = df.count()
        if total == 0:
            return DQResult(f"null_rate:{column}", self.table, True, 0.0, max_null_pct,
                            "Table is empty — skipping null check.")
        nulls     = df.filter(F.col(column).isNull()).count()
        null_pct  = (nulls / total) * 100
        passed    = null_pct <= max_null_pct
        result    = DQResult(
            check_name=f"null_rate:{column}",
            table=self.table,
            passed=passed,
            value=round(null_pct, 2),
            threshold=max_null_pct,
            message=f"{nulls}/{total} rows have NULL {column} ({null_pct:.2f}%)"
        )
        self.results.append(result)
        log_fn = logger.info if passed else logger.warning
        log_fn("[DQ] %s — %s", "PASS" if passed else "FAIL", result.message)
        return result

    def check_duplicate_rate(self, key_col: str = "id", max_dup_pct: float = 0.1) -> DQResult:
        """Fail if the duplicate rate on `key_col` exceeds `max_dup_pct` %."""
        df    = self._load()
        total = df.count()
        if total == 0:
            return DQResult(f"dup_rate:{key_col}", self.table, True, 0.0, max_dup_pct)
        distinct  = df.select(key_col).distinct().count()
        dup_pct   = ((total - distinct) / total) * 100
        passed    = dup_pct <= max_dup_pct
        result    = DQResult(
            check_name=f"dup_rate:{key_col}",
            table=self.table,
            passed=passed,
            value=round(dup_pct, 4),
            threshold=max_dup_pct,
            message=f"{total - distinct} duplicates in {total} rows ({dup_pct:.4f}%)"
        )
        self.results.append(result)
        log_fn = logger.info if passed else logger.warning
        log_fn("[DQ] %s — %s", "PASS" if passed else "FAIL", result.message)
        return result

    def check_temperature_range(
        self, col: str = "weather_temperature",
        min_val: float = -50.0, max_val: float = 60.0
    ) -> DQResult:
        """Fail if any non-null temperature is outside the physical range."""
        df      = self._load().filter(F.col(col).isNotNull())
        total   = df.count()
        outliers = df.filter((F.col(col) < min_val) | (F.col(col) > max_val)).count()
        pct     = (outliers / total * 100) if total > 0 else 0.0
        passed  = outliers == 0
        result  = DQResult(
            check_name=f"outlier:{col}",
            table=self.table,
            passed=passed,
            value=float(outliers),
            threshold=0.0,
            message=f"{outliers} rows outside [{min_val}, {max_val}] °C"
        )
        self.results.append(result)
        log_fn = logger.info if passed else logger.error
        log_fn("[DQ] %s — %s", "PASS" if passed else "FAIL", result.message)
        return result

    def check_freshness(self, ts_col: str = "event_timestamp", max_age_minutes: int = 60) -> DQResult:
        """Fail if the most recent event is older than `max_age_minutes`."""
        df = self._load()
        latest_row = df.agg(F.max(ts_col).alias("latest")).collect()[0]
        latest = latest_row["latest"]
        if latest is None:
            return DQResult(
                f"freshness:{ts_col}", self.table, False, float("inf"), max_age_minutes,
                "No records found in the table."
            )
        from datetime import datetime, timezone
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        age_minutes = (now_utc - latest).total_seconds() / 60
        passed = age_minutes <= max_age_minutes
        result = DQResult(
            check_name=f"freshness:{ts_col}",
            table=self.table,
            passed=passed,
            value=round(age_minutes, 2),
            threshold=float(max_age_minutes),
            message=f"Latest event is {age_minutes:.1f} min old (max allowed: {max_age_minutes} min)"
        )
        self.results.append(result)
        log_fn = logger.info if passed else logger.error
        log_fn("[DQ] %s — %s", "PASS" if passed else "FAIL", result.message)
        return result

    def run_all_checks(self) -> List[DQResult]:
        """Run all standard DQ checks and return results."""
        logger.info("Running DQ checks on: %s", self.table)
        self.check_null_rate("id",               max_null_pct=0.0)   # id must never be null
        self.check_null_rate("event_timestamp",  max_null_pct=1.0)
        self.check_null_rate("zone",             max_null_pct=5.0)
        self.check_null_rate("weather_temperature", max_null_pct=20.0)
        self.check_duplicate_rate("id",          max_dup_pct=0.01)
        self.check_temperature_range()
        self.check_freshness(max_age_minutes=30)

        failed = [r for r in self.results if not r.passed]
        logger.info("DQ Summary: %d checks, %d failed.", len(self.results), len(failed))
        return self.results


def run_pipeline_status():
    """Print row counts and snapshot info for all Medallion tables."""
    spark = create_spark_session(app_name="SmartCity-Pipeline-Status")

    tables = {
        "Bronze": BRONZE_TABLE,
        "Silver": SILVER_TABLE,
        "Gold-Hourly": GOLD_HOURLY_TABLE,
        "Gold-Emergency": GOLD_EMERGENCY_TABLE,
    }

    print("\n" + "=" * 70)
    print(f"{'Layer':<15} {'Table':<45} {'Row Count':>10}")
    print("=" * 70)

    for layer, table in tables.items():
        try:
            count = spark.read.format("iceberg").load(table).count()
            print(f"{layer:<15} {table:<45} {count:>10,}")
        except Exception as e:
            print(f"{layer:<15} {table:<45} {'ERROR: ' + str(e)[:20]:>10}")

    print("=" * 70)

    # Show snapshot history for Silver (most recent 5)
    print(f"\nSnapshot history for {SILVER_TABLE} (last 5):")
    try:
        spark.sql(f"SELECT * FROM {SILVER_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 5") \
             .select("snapshot_id", "committed_at", "operation", "summary") \
             .show(truncate=False)
    except Exception as e:
        print(f"Could not read snapshots: {e}")


def verify_nessie_catalog(spark: SparkSession = None):
    """Confirm Nessie connectivity and list all registered tables."""
    if spark is None:
        spark = create_spark_session(app_name="SmartCity-Nessie-Verify")

    print("\nNessie catalog tables:")
    spark.sql("SHOW TABLES IN nessie").show(truncate=False)

    print("\nNessie branches:")
    spark.sql("LIST REFERENCES IN nessie").show(truncate=False)


if __name__ == "__main__":
    run_pipeline_status()
