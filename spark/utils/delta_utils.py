"""
Shared Delta Lake utilities.

Centralises the SparkSession builder and common MERGE helpers so every
job gets consistent config without copy-paste.
"""

import os
from typing import Dict, List, Optional

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str, master: Optional[str] = None) -> SparkSession:
    """
    Build a SparkSession pre-configured for:
      - Delta Lake (extensions + catalog)
      - S3A → MinIO (path-style access)
      - Adaptive Query Execution
    """
    master_url = master or os.getenv("SPARK_MASTER_URL", "local[*]")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        # ── Delta Lake ──────────────────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # ── S3A / MinIO ─────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        # ── Performance ─────────────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_condition: str,
    update_set: Dict[str, str],
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Generic MERGE (upsert) into a Delta table.

    Creates the table on first run; subsequent runs merge.
    This is the core pattern for handling late-arriving events.

    Args:
        source_df:       New / updated records.
        target_path:     S3A path to the Delta table.
        merge_condition: SQL condition joining target and source (e.g. "t.job_id = s.job_id").
        update_set:      Columns to update on match {"target_col": "source_expr"}.
        partition_by:    Optional partition columns for new table creation.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First write — create the table
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)


def optimize_delta_table(spark: SparkSession, path: str, z_order_cols: List[str]) -> None:
    """
    Run OPTIMIZE + ZORDER on a Delta table.

    Call this after large writes to improve read performance.
    Typically run once daily in the Airflow DAG.
    """
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({', '.join(z_order_cols)})")


def vacuum_delta_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """
    Remove old Delta log files beyond the retention window.
    Default: keep 7 days of history (168 hours).
    """
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
