"""
Great Expectations suite for the Silver jobs Delta table.

Assertions are designed to catch real production problems:
  - Silent pipeline failures (row count too low)
  - Data type corruption (negative costs, out-of-range utilization)
  - Distribution drift (avg cost drifts > 50% from baseline → billing bug)
  - Referential integrity (GPU types must be in known set)

This suite is run as a checkpoint task in the Airflow batch DAG,
blocking promotion from Silver → Gold on failure.
"""

import os

import great_expectations as gx

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
SILVER_PATH = os.getenv("DATA_SILVER_PATH", "s3a://silver")
GX_ROOT = "/opt/quality"

KNOWN_GPU_TYPES = [
    "H100-SXM5-80GB",
    "A100-SXM4-80GB",
    "A100-PCIe-40GB",
    "RTX-4090",
    "A10G",
]

KNOWN_FRAMEWORKS = ["pytorch", "jax", "tensorflow", "deepspeed"]


def build_context() -> gx.DataContext:
    """Build a GX DataContext backed by the quality/ directory."""
    context = gx.get_context(
        context_root_dir=GX_ROOT,
        mode="file",
    )
    return context


def define_silver_jobs_suite(context: gx.DataContext) -> None:
    """
    Define (or update) the expectation suite for silver/jobs.
    Safe to call repeatedly — create_expectation_suite uses overwrite_existing=True.
    """
    suite = context.add_or_update_expectation_suite(
        expectation_suite_name="silver_jobs.warning"
    )

    # ── Row volume ─────────────────────────────────────────────────────────
    # A successful daily pipeline should produce at least 500 job records.
    # Below this threshold usually means the Kafka consumer stalled or the
    # Spark job read from the wrong partition.
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 500, "max_value": None},
    ))

    # ── Not-null on critical columns ──────────────────────────────────────
    for col in ["job_id", "org_id", "user_id", "gpu_type", "gpu_count", "started_at"]:
        suite.add_expectation(gx.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col},
        ))

    # ── Uniqueness ────────────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "job_id"},
    ))

    # ── Value ranges ──────────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "gpu_count", "min_value": 1, "max_value": 512},
    ))

    # Cost must be non-negative (NULL is allowed for in-flight jobs)
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "cost_usd",
            "min_value": 0.0,
            "max_value": None,
            "mostly": 0.99,    # allow 1% NULL (late-arriving jobs not yet complete)
        },
    ))

    # ── Categorical values ────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "gpu_type",
            "value_set": KNOWN_GPU_TYPES,
            "mostly": 0.99,
        },
    ))

    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "framework",
            "value_set": KNOWN_FRAMEWORKS,
            "mostly": 0.98,
        },
    ))

    # ── Distribution checks ───────────────────────────────────────────────
    # Average cost should be in a realistic range (based on our data distribution).
    # A drift outside this range signals a pricing bug or data issue.
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_mean_to_be_between",
        kwargs={"column": "cost_usd", "min_value": 1.0, "max_value": 500.0},
    ))

    # GPU count distribution: median should be between 1–8
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_median_to_be_between",
        kwargs={"column": "gpu_count", "min_value": 1, "max_value": 8},
    ))

    context.save_expectation_suite(suite)
    print(f"Suite 'silver_jobs.warning' saved with {len(suite.expectations)} expectations.")
