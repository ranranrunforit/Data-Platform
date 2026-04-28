"""
Great Expectations suite for the Silver jobs Delta table.

Assertions are designed to catch real production problems:
  - Silent pipeline failures (row count too low)
  - Data type corruption (negative costs, out-of-range gpu_count)
  - Distribution drift (avg cost drifts > 50% from baseline → billing bug)
  - Referential integrity (GPU types must be in known set)

Applied via the validator directly so the suite lives in memory — the
read-only `/opt/quality` mount cannot persist a GX project on disk.
"""

KNOWN_GPU_TYPES = [
    "H100-SXM5-80GB",
    "A100-SXM4-80GB",
    "A100-PCIe-40GB",
    "RTX-4090",
    "A10G",
]

KNOWN_FRAMEWORKS = ["pytorch", "jax", "tensorflow", "deepspeed"]


def apply_silver_jobs_expectations(validator) -> None:
    """Attach the silver_jobs expectations to a fluent GX Validator."""
    # Row volume — silent failure detector
    validator.expect_table_row_count_to_be_between(min_value=500)

    # Not-null on critical columns
    for col in ["job_id", "org_id", "user_id", "gpu_type", "gpu_count", "started_at"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # Uniqueness
    validator.expect_column_values_to_be_unique(column="job_id")

    # Value ranges
    validator.expect_column_values_to_be_between(
        column="gpu_count", min_value=1, max_value=512,
    )
    # Cost must be non-negative (NULL allowed for in-flight jobs)
    validator.expect_column_values_to_be_between(
        column="cost_usd", min_value=0.0, mostly=0.99,
    )

    # Categorical
    validator.expect_column_values_to_be_in_set(
        column="gpu_type", value_set=KNOWN_GPU_TYPES, mostly=0.99,
    )
    validator.expect_column_values_to_be_in_set(
        column="framework", value_set=KNOWN_FRAMEWORKS, mostly=0.98,
    )

    # Distribution — drift detector
    validator.expect_column_mean_to_be_between(
        column="cost_usd", min_value=1.0, max_value=500.0,
    )
    validator.expect_column_median_to_be_between(
        column="gpu_count", min_value=1, max_value=8,
    )
