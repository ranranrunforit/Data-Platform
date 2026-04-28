"""
Bronze → Silver transformation (Apache Spark / PySpark).

Reads raw JSON events from MinIO bronze bucket, applies:
  1. Schema enforcement + type casting
  2. Deduplication (idempotent on re-run)
  3. MERGE for late-arriving job completion events
  4. Data enrichment (GPU pricing tier, org metadata)

Writes Delta Lake tables to MinIO silver bucket.

Run via spark-submit:
  spark-submit --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.1.0,... \
    spark/jobs/bronze_to_silver.py
"""

import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, "/opt/spark-apps")
from utils.delta_utils import get_spark_session, upsert_to_delta

BRONZE = os.getenv("DATA_BRONZE_PATH", "s3a://bronze")
SILVER = os.getenv("DATA_SILVER_PATH", "s3a://silver")

# ── GPU pricing reference (denormalised into silver for query convenience) ────
GPU_PRICING = {
    "H100-SXM5-80GB": 4.25,
    "A100-SXM4-80GB": 3.00,
    "A100-PCIe-40GB":  2.21,
    "RTX-4090":        0.74,
    "A10G":            0.90,
}

# ── Schemas ───────────────────────────────────────────────────────────────────

JOB_SCHEMA = StructType([
    StructField("job_id",          StringType(),    False),
    StructField("org_id",          StringType(),    True),
    StructField("user_id",         StringType(),    True),
    StructField("gpu_type",        StringType(),    True),
    StructField("gpu_count",       IntegerType(),   True),
    StructField("framework",       StringType(),    True),
    StructField("model_arch",      StringType(),    True),
    StructField("dataset_size_gb", DoubleType(),    True),
    StructField("started_at",      TimestampType(), True),
    StructField("ended_at",        TimestampType(), True),
    StructField("duration_hours",  DoubleType(),    True),
    StructField("exit_code",       IntegerType(),   True),
    StructField("cost_usd",        DoubleType(),    True),
    StructField("is_late_arrival", StringType(),    True),
])

COMPLETION_SCHEMA = StructType([
    StructField("job_id",          StringType(),    False),
    StructField("ended_at",        TimestampType(), True),
    StructField("duration_hours",  DoubleType(),    True),
    StructField("exit_code",       IntegerType(),   True),
    StructField("cost_usd",        DoubleType(),    True),
    StructField("late_delay_hours", DoubleType(),   True),
])

INFERENCE_SCHEMA = StructType([
    StructField("request_id",    StringType(),    False),
    StructField("org_id",        StringType(),    True),
    StructField("user_id",       StringType(),    True),
    StructField("model_id",      StringType(),    True),
    StructField("region",        StringType(),    True),
    StructField("status_code",   IntegerType(),   True),
    StructField("cache_hit",     StringType(),    True),
    StructField("input_tokens",  IntegerType(),   True),
    StructField("output_tokens", IntegerType(),   True),
    StructField("total_tokens",  IntegerType(),   True),
    StructField("latency_ms",    DoubleType(),    True),
    StructField("cost_usd",      DoubleType(),    True),
    StructField("timestamp",     TimestampType(), True),
])


# ── Transformations ───────────────────────────────────────────────────────────

def transform_jobs(spark: SparkSession) -> DataFrame:
    """
    Read raw job start events and apply cleaning + enrichment.
    Returns a DataFrame ready for upsert into silver/jobs.
    """
    raw = (
        spark.read
        .schema(JOB_SCHEMA)
        .json(f"{BRONZE}/job_events/")
    )

    # Drop exact duplicates (idempotent re-runs)
    deduped = raw.dropDuplicates(["job_id"])

    # Enrich: add price_per_gpu_hour for cost auditing
    pricing_map = F.create_map(
        *[item for pair in
          [(F.lit(k), F.lit(v)) for k, v in GPU_PRICING.items()]
          for item in pair]
    )

    enriched = (
        deduped
        .withColumn("price_per_gpu_hour", pricing_map[F.col("gpu_type")])
        .withColumn("is_late_arrival", F.col("is_late_arrival").cast("boolean"))
        .withColumn("is_success",
                    F.when(F.col("exit_code") == 0, True)
                    .when(F.col("exit_code").isNull(), None)
                    .otherwise(False))
        .withColumn("gpu_hours",
                    F.when(F.col("duration_hours").isNotNull(),
                           F.col("duration_hours") * F.col("gpu_count"))
                    .otherwise(None))
        # Partition column: date of job start (used for Delta partitioning)
        .withColumn("job_date", F.to_date(F.col("started_at")))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    return enriched


def transform_completions(spark: SparkSession) -> DataFrame:
    """
    Read late-arriving completion events.
    These will MERGE into silver/jobs to fill in ended_at, cost_usd, etc.
    """
    return (
        spark.read
        .schema(COMPLETION_SCHEMA)
        .json(f"{BRONZE}/job_completions/")
        .dropDuplicates(["job_id"])
    )


def transform_inference(spark: SparkSession) -> DataFrame:
    """
    Read raw inference API logs and apply cleaning + enrichment.
    """
    raw = (
        spark.read
        .schema(INFERENCE_SCHEMA)
        .json(f"{BRONZE}/inference_logs/")
    )

    return (
        raw
        .dropDuplicates(["request_id"])
        .withColumn("cache_hit", F.col("cache_hit").cast("boolean"))
        .withColumn("is_success", F.col("status_code") == 200)
        .withColumn("log_date", F.to_date(F.col("timestamp")))
        .withColumn("log_hour", F.hour(F.col("timestamp")))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ── MERGE: late-arriving completions ─────────────────────────────────────────

def merge_completions(spark: SparkSession, completions: DataFrame) -> None:
    """
    The key interview pattern: upsert completion events into silver/jobs.

    In distributed job schedulers, the scheduler emits a start event immediately
    but the completion event may arrive hours later (or not at all for OOM kills).
    A plain INSERT would create duplicate rows. MERGE handles this correctly.

    Match condition: same job_id AND target doesn't yet have a completion.
    Only update: don't overwrite an already-completed record.
    """
    from delta import DeltaTable

    silver_jobs_path = f"{SILVER}/jobs"

    if not DeltaTable.isDeltaTable(spark, silver_jobs_path):
        print("Silver jobs table doesn't exist yet — skipping completion merge.")
        return

    target = DeltaTable.forPath(spark, silver_jobs_path)
    (
        target.alias("t")
        .merge(
            completions.alias("s"),
            "t.job_id = s.job_id AND t.ended_at IS NULL",
        )
        .whenMatchedUpdate(set={
            "ended_at":        "s.ended_at",
            "duration_hours":  "s.duration_hours",
            "exit_code":       "s.exit_code",
            "cost_usd":        "s.cost_usd",
            "gpu_hours":       "s.duration_hours * t.gpu_count",
            "is_success":      "s.exit_code = 0",
            "_ingested_at":    "current_timestamp()",
        })
        .execute()
    )
    print("Completion MERGE complete.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.utcnow().isoformat()}] Starting bronze → silver...")
    spark = get_spark_session("bronze_to_silver")

    try:
        # ── Jobs ──────────────────────────────────────────────────────────
        print("Transforming job events...")
        jobs_df = transform_jobs(spark)
        jobs_count = jobs_df.count()
        print(f"  {jobs_count:,} job records after dedup")

        upsert_to_delta(
            spark=spark,
            source_df=jobs_df,
            target_path=f"{SILVER}/jobs",
            merge_condition="t.job_id = s.job_id",
            update_set={
                "ended_at":       "s.ended_at",
                "duration_hours": "s.duration_hours",
                "exit_code":      "s.exit_code",
                "cost_usd":       "s.cost_usd",
                "gpu_hours":      "s.gpu_hours",
                "is_success":     "s.is_success",
                "_ingested_at":   "s._ingested_at",
            },
            partition_by=["job_date"],
            overwrite=True,
        )
        print(f"  Written to {SILVER}/jobs (Delta)")

        # ── Completion events (late arrivals) ─────────────────────────────
        print("Merging late-arriving completion events...")
        completions_df = transform_completions(spark)
        comp_count = completions_df.count()
        print(f"  {comp_count:,} completion events to merge")
        merge_completions(spark, completions_df)

        # ── Inference logs ────────────────────────────────────────────────
        print("Transforming inference logs...")
        inference_df = transform_inference(spark)
        inf_count = inference_df.count()
        print(f"  {inf_count:,} inference records after dedup")

        upsert_to_delta(
            spark=spark,
            source_df=inference_df,
            target_path=f"{SILVER}/inference",
            merge_condition="t.request_id = s.request_id",
            update_set={
                "status_code":   "s.status_code",
                "latency_ms":    "s.latency_ms",
                "cost_usd":      "s.cost_usd",
                "_ingested_at":  "s._ingested_at",
            },
            partition_by=["log_date"],
            overwrite=True,
        )
        print(f"  Written to {SILVER}/inference (Delta)")

        print(f"[{datetime.utcnow().isoformat()}] bronze → silver complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
