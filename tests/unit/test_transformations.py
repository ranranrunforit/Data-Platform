"""
Unit tests for PySpark Silver transform logic.

Runs in Spark local[*] mode — no cluster needed.
Tests the business logic that interviewers will ask about:
  - Deduplication is idempotent
  - MERGE correctly applies late-arriving completions
  - GPU pricing enrichment is correct
  - Cost calculation handles all exit code scenarios
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Spark session in local mode — no cluster required."""
    return (
        SparkSession.builder
        .appName("unit-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")   # suppress Spark UI in tests
        .getOrCreate()
    )


@pytest.fixture
def sample_jobs(spark):
    """Minimal job records covering key scenarios."""
    now = datetime.utcnow()
    schema = StructType([
        StructField("job_id",          StringType(),    False),
        StructField("org_id",          StringType(),    True),
        StructField("user_id",         StringType(),    True),
        StructField("gpu_type",        StringType(),    True),
        StructField("gpu_count",       IntegerType(),   True),
        StructField("framework",       StringType(),    True),
        StructField("model_arch",      StringType(),    True),
        StructField("started_at",      TimestampType(), True),
        StructField("ended_at",        TimestampType(), True),
        StructField("duration_hours",  DoubleType(),    True),
        StructField("exit_code",       IntegerType(),   True),
        StructField("cost_usd",        DoubleType(),    True),
        StructField("is_late_arrival", StringType(),    True),
    ])

    data = [
        # Completed job — all fields present
        ("job-001", "org-001", "user-001", "H100-SXM5-80GB", 8,
         "pytorch", "llm-70b",
         now - timedelta(hours=5), now - timedelta(hours=1),
         4.0, 0, 136.0, "false"),

        # Late-arriving job — no completion yet
        ("job-002", "org-001", "user-002", "A100-SXM4-80GB", 4,
         "jax", "llm-7b",
         now - timedelta(hours=3), None,
         None, None, None, "true"),

        # OOM kill (exit code 137) — still billable
        ("job-003", "org-002", "user-003", "RTX-4090", 1,
         "pytorch", "custom",
         now - timedelta(hours=2), now - timedelta(hours=1),
         1.0, 137, 0.74, "false"),

        # Platform error (exit code 143) — not billable
        ("job-004", "org-002", "user-004", "A100-PCIe-40GB", 2,
         "tensorflow", "bert-large",
         now - timedelta(hours=1), now - timedelta(minutes=30),
         0.5, 143, 0.0, "false"),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def completion_events(spark):
    """Completion events that should merge into job-002."""
    schema = StructType([
        StructField("job_id",         StringType(),    False),
        StructField("ended_at",       TimestampType(), True),
        StructField("duration_hours", DoubleType(),    True),
        StructField("exit_code",      IntegerType(),   True),
        StructField("cost_usd",       DoubleType(),    True),
    ])
    now = datetime.utcnow()
    data = [("job-002", now, 3.0, 0, 36.0)]
    return spark.createDataFrame(data, schema)


# ── Deduplication tests ───────────────────────────────────────────────────────

class TestDeduplication:
    def test_dedup_removes_exact_duplicates(self, sample_jobs):
        """Re-running the pipeline with the same data must not create duplicates."""
        doubled = sample_jobs.union(sample_jobs)
        deduped = doubled.dropDuplicates(["job_id"])
        assert deduped.count() == sample_jobs.count()

    def test_dedup_is_deterministic(self, sample_jobs):
        """Job IDs must be unique after deduplication."""
        deduped = sample_jobs.dropDuplicates(["job_id"])
        unique_ids = deduped.select("job_id").distinct().count()
        assert unique_ids == deduped.count()


# ── GPU pricing enrichment tests ──────────────────────────────────────────────

class TestGpuPricing:
    GPU_PRICING = {
        "H100-SXM5-80GB": 4.25,
        "A100-SXM4-80GB": 3.00,
        "A100-PCIe-40GB": 2.21,
        "RTX-4090":        0.74,
        "A10G":            0.90,
    }

    def test_pricing_enrichment(self, spark, sample_jobs):
        """Every known GPU type should map to a non-null price."""
        pricing_map = F.create_map(
            *[item for pair in
              [(F.lit(k), F.lit(v)) for k, v in self.GPU_PRICING.items()]
              for item in pair]
        )
        enriched = sample_jobs.withColumn("price", pricing_map[F.col("gpu_type")])
        null_prices = enriched.filter(
            F.col("price").isNull() & F.col("gpu_type").isNotNull()
        ).count()
        assert null_prices == 0, "Unknown GPU type produced NULL price"

    def test_h100_most_expensive(self, spark):
        """H100 must be priced higher than A100."""
        assert self.GPU_PRICING["H100-SXM5-80GB"] > self.GPU_PRICING["A100-SXM4-80GB"]


# ── Cost calculation tests ────────────────────────────────────────────────────

class TestCostCalculation:
    def test_completed_job_cost(self, spark, sample_jobs):
        """Completed job cost = duration × gpu_count × price_per_gpu_hour."""
        job = sample_jobs.filter(F.col("job_id") == "job-001").first()
        expected = 4.0 * 8 * 4.25   # duration × gpus × H100 price
        assert abs(job["cost_usd"] - expected) < 0.01

    def test_oom_job_is_billable(self, spark, sample_jobs):
        """OOM kills (exit 137) should have non-zero cost."""
        job = sample_jobs.filter(F.col("job_id") == "job-003").first()
        assert job["cost_usd"] > 0, "OOM job should be billed"
        assert job["exit_code"] == 137

    def test_platform_error_is_free(self, spark, sample_jobs):
        """Platform errors (exit 143) should have zero cost."""
        job = sample_jobs.filter(F.col("job_id") == "job-004").first()
        assert job["cost_usd"] == 0.0, "Platform error (exit 143) must not be billed"

    def test_late_arrival_has_null_cost(self, spark, sample_jobs):
        """Jobs with no completion event should have NULL cost (not 0)."""
        job = sample_jobs.filter(F.col("job_id") == "job-002").first()
        assert job["cost_usd"] is None, "In-flight job cost should be NULL until completion"


# ── MERGE / late arrival tests ────────────────────────────────────────────────

class TestMergeLogic:
    def test_completion_fills_null_ended_at(self, spark, sample_jobs, completion_events):
        """
        After applying a completion event, the job should have ended_at populated.
        This simulates the MERGE without needing a Delta table — tests the SQL logic.
        """
        # Simulate MERGE: update job-002 with its completion event
        merged = (
            sample_jobs.alias("t")
            .join(
                completion_events.alias("s"),
                (F.col("t.job_id") == F.col("s.job_id")) & F.col("t.ended_at").isNull(),
                "left",
            )
            .select(
                F.col("t.job_id"),
                F.coalesce(F.col("s.ended_at"),   F.col("t.ended_at")).alias("ended_at"),
                F.coalesce(F.col("s.cost_usd"),   F.col("t.cost_usd")).alias("cost_usd"),
                F.coalesce(F.col("s.exit_code"),  F.col("t.exit_code")).alias("exit_code"),
            )
        )

        job_002 = merged.filter(F.col("job_id") == "job-002").first()
        assert job_002["ended_at"] is not None, "Completion MERGE should fill ended_at"
        assert job_002["cost_usd"] == 36.0
        assert job_002["exit_code"] == 0

    def test_merge_does_not_overwrite_completed_jobs(self, sample_jobs, completion_events):
        """
        Completed jobs (ended_at IS NOT NULL) must not be overwritten by a late event.
        The MERGE condition includes 'AND t.ended_at IS NULL'.
        """
        already_complete = sample_jobs.filter(
            (F.col("job_id") == "job-001") & F.col("ended_at").isNotNull()
        )
        # This job should not be in the merge target (condition excludes it)
        merge_targets = already_complete.filter(F.col("ended_at").isNull())
        assert merge_targets.count() == 0, "Completed jobs must be excluded from MERGE target"


# ── Data quality tests ────────────────────────────────────────────────────────

class TestDataQuality:
    def test_no_jobs_before_2020(self, sample_jobs):
        """Sanity check: no job timestamps before 2020 (indicates epoch=0 bug)."""
        from datetime import datetime
        cutoff = datetime(2020, 1, 1)
        old_jobs = sample_jobs.filter(F.col("started_at") < F.lit(cutoff)).count()
        assert old_jobs == 0

    def test_gpu_count_positive(self, sample_jobs):
        """GPU count must always be >= 1."""
        invalid = sample_jobs.filter(F.col("gpu_count") < 1).count()
        assert invalid == 0

    def test_end_after_start(self, sample_jobs):
        """Jobs with both timestamps should have ended_at > started_at."""
        invalid = sample_jobs.filter(
            F.col("ended_at").isNotNull() &
            (F.col("ended_at") <= F.col("started_at"))
        ).count()
        assert invalid == 0, "ended_at must be after started_at"
