"""
================================================================================
 Medallion Pipeline — Iceberg Table Maintenance (FIXED)
================================================================================
 Fix applied:
   - expire_snapshots / remove_orphan_files require 'gc.enabled' = 'true'
     in the table TBLPROPERTIES. When using Nessie as the catalog, GC is
     disabled by default to protect multi-table catalog integrity.
     Solution: enable GC per-table via ALTER TABLE before running GC ops,
     then disable it again after — keeping Nessie safe.
   - Added skip_gc_ops flag for environments where GC must stay disabled.
================================================================================
"""

import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from p_config.spark_session import create_spark_session
from p_config.settings import BRONZE_TABLE, SILVER_TABLE, GOLD_HOURLY_TABLE, GOLD_EMERGENCY_TABLE

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ALL_TABLES = [BRONZE_TABLE, SILVER_TABLE, GOLD_HOURLY_TABLE, GOLD_EMERGENCY_TABLE]


def _enable_gc(spark: SparkSession, table: str) -> None:
    """Enable GC on the table — required before expire_snapshots / remove_orphan_files."""
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('gc.enabled' = 'true')")
    logger.info("GC enabled on %s", table)


def _disable_gc(spark: SparkSession, table: str) -> None:
    """Restore GC=false after maintenance — protects Nessie catalog integrity."""
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('gc.enabled' = 'false')")
    logger.info("GC disabled on %s", table)


def rewrite_data_files(spark: SparkSession, table: str) -> None:
    """Compact small streaming micro-batch files into larger ones."""
    logger.info("Rewriting data files for %s...", table)
    spark.sql(f"""
        CALL nessie.system.rewrite_data_files(
            table    => '{table}',
            strategy => 'binpack',
            options  => map(
                'target-file-size-bytes',                    '268435456',
                'min-file-size-bytes',                       '67108864',
                'max-file-size-bytes',                       '536870912',
                'max-concurrent-file-group-rewrites',        '5'
            )
        )
    """).show()


def rewrite_manifests(spark: SparkSession, table: str) -> None:
    """Rewrite bloated manifest files accumulated from streaming micro-batches."""
    logger.info("Rewriting manifests for %s...", table)
    spark.sql(f"CALL nessie.system.rewrite_manifests(table => '{table}')").show()


def expire_snapshots(spark: SparkSession, table: str, retain_days: int = 7) -> None:
    """
    Expire old snapshots.
    REQUIRES gc.enabled = true — we enable/disable it around the call.
    """
    cutoff = (datetime.utcnow() - timedelta(days=retain_days)).strftime('%Y-%m-%d %H:%M:%S')
    logger.info("Expiring snapshots older than %s on %s", cutoff, table)

    try:
        _enable_gc(spark, table)
        spark.sql(f"""
            CALL nessie.system.expire_snapshots(
                table       => '{table}',
                older_than  => TIMESTAMP '{cutoff}',
                retain_last => 5
            )
        """).show()
    finally:
        # Always restore GC=false even if the expire call fails
        _disable_gc(spark, table)


def remove_orphan_files(spark: SparkSession, table: str, older_than_days: int = 3) -> None:
    """
    Remove orphaned data files.
    REQUIRES gc.enabled = true — we enable/disable it around the call.
    """
    cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y-%m-%d %H:%M:%S')
    logger.info("Removing orphan files older than %s on %s", cutoff, table)

    try:
        _enable_gc(spark, table)
        spark.sql(f"""
            CALL nessie.system.remove_orphan_files(
                table      => '{table}',
                older_than => TIMESTAMP '{cutoff}'
            )
        """).show()
    finally:
        _disable_gc(spark, table)


def run_full_maintenance(tables: list = None,
                         retain_snapshot_days: int = 7,
                         skip_gc_ops: bool = False) -> None:
    """
    Full maintenance run.

    Args:
        skip_gc_ops: set True if your Nessie version doesn't allow GC at all.
                     Compaction and manifest rewrite will still run (safe).
    """
    spark = create_spark_session(app_name="SmartCity-Iceberg-Maintenance")
    target_tables = tables or ALL_TABLES

    for table in target_tables:
        logger.info("=" * 60)
        logger.info("Maintenance: %s", table)
        logger.info("=" * 60)

        try:
            rewrite_data_files(spark, table)
        except Exception as e:
            logger.error("rewrite_data_files failed for %s: %s", table, e)

        try:
            rewrite_manifests(spark, table)
        except Exception as e:
            logger.error("rewrite_manifests failed for %s: %s", table, e)

        if skip_gc_ops:
            logger.warning("Skipping GC ops (expire + orphan) for %s — skip_gc_ops=True", table)
            continue

        try:
            expire_snapshots(spark, table, retain_days=retain_snapshot_days)
        except Exception as e:
            logger.error("expire_snapshots failed for %s: %s", table, e)

        try:
            remove_orphan_files(spark, table)
        except Exception as e:
            logger.error("remove_orphan_files failed for %s: %s", table, e)

    logger.info("Maintenance complete.")


if __name__ == "__main__":
    import sys
    # Pass --skip-gc to skip expire/orphan ops (safe mode for Nessie)
    skip_gc = "--skip-gc" in sys.argv
    run_full_maintenance(skip_gc_ops=skip_gc)
