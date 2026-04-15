"""
Delta Lake table optimization — run nightly after dbt Gold completes.

OPTIMIZE + ZORDER improves read performance by:
  1. Compacting many small files into fewer large ones
  2. Z-ordering (multi-dimensional clustering) for common query predicates

This is called by the Airflow batch DAG as the final step.
"""

import os
import sys

sys.path.insert(0, "/opt/spark-apps")
from utils.delta_utils import get_spark_session, optimize_delta_table, vacuum_delta_table

SILVER = os.getenv("DATA_SILVER_PATH", "s3a://silver")
GOLD = os.getenv("DATA_GOLD_PATH", "s3a://gold")


def main():
    spark = get_spark_session("optimize_tables")

    try:
        tables = [
            # (path, z_order_columns)
            (f"{SILVER}/jobs",                     ["org_id", "job_date"]),
            (f"{SILVER}/inference",                ["model_id", "log_date"]),
            (f"{GOLD}/cost_attribution",           ["org_id", "job_date"]),
            (f"{GOLD}/gpu_utilization_hourly",     ["gpu_type", "metric_date"]),
            (f"{GOLD}/job_performance_sla",        ["model_id", "log_date"]),
        ]

        for path, z_cols in tables:
            print(f"Optimizing {path} (Z-ORDER by {z_cols})...")
            optimize_delta_table(spark, path, z_cols)
            print("  Done.")

        # Vacuum old files (keep 7 days of history)
        print("Vacuuming Silver tables...")
        for path, _ in tables[:2]:
            vacuum_delta_table(spark, path, retention_hours=168)

        print("Optimization complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
