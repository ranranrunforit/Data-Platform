"""
Great Expectations checkpoint for the Silver layer.

Called by the Airflow DAG between the Spark Silver write and the dbt Gold run.
If ANY critical expectation fails, this script exits with code 1,
which causes the Airflow task to fail and blocks Gold promotion.

Usage:
    python quality/checkpoints/silver_checkpoint.py
    # Exit 0 = passed, exit 1 = failed (Airflow marks task as failed)
"""

import os
import sys

import duckdb
import great_expectations as gx
import pandas as pd

sys.path.insert(0, "/opt/quality")
from expectations.suite_silver_jobs import build_context, define_silver_jobs_suite

SILVER_PATH = os.getenv("DATA_SILVER_PATH", "s3a://silver")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")


def load_silver_jobs_sample() -> pd.DataFrame:
    """
    Load the silver jobs table into a Pandas DataFrame via DuckDB.
    DuckDB reads Delta files directly — no Spark needed for validation.
    """
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{AWS_KEY}';
        SET s3_secret_access_key='{AWS_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    df = con.execute(f"""
        SELECT
            job_id, org_id, user_id, gpu_type, gpu_count,
            framework, model_arch, started_at, ended_at,
            cost_usd, exit_code, is_success, is_late_arrival
        FROM delta_scan('{SILVER_PATH}/jobs')
        LIMIT 100000
    """).df()

    con.close()
    print(f"Loaded {len(df):,} rows from {SILVER_PATH}/jobs for GX validation.")
    return df


def run_checkpoint() -> bool:
    """
    Runs the silver_jobs GX suite against a sample of the Silver table.
    Returns True if all CRITICAL expectations pass, False otherwise.
    """
    context = build_context()
    define_silver_jobs_suite(context)

    df = load_silver_jobs_sample()

    # Use PandasDataset validator (simple, no Spark needed for validation)
    validator = context.get_validator(
        batch_request=gx.core.batch.RuntimeBatchRequest(
            datasource_name="silver_pandas",
            data_connector_name="runtime_data_connector",
            data_asset_name="silver_jobs",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": "airflow_batch"},
        ),
        expectation_suite_name="silver_jobs.warning",
    )

    results = validator.validate()
    success = results.success

    # ── Print summary ─────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"GX Validation Result: {'PASSED' if success else 'FAILED'}")
    print("=" * 60)

    passed = sum(1 for r in results.results if r.success)
    failed = sum(1 for r in results.results if not r.success)
    print(f"  Expectations: {passed} passed, {failed} failed")

    if not success:
        print("\nFailed expectations:")
        for r in results.results:
            if not r.success:
                print(f"  FAIL  {r.expectation_config.expectation_type}")
                print(f"        {r.result}")

    # Save Data Docs (human-readable HTML report)
    context.build_data_docs()
    print(f"\nData Docs: {os.path.join('/opt/quality', 'uncommitted/data_docs/local_site/index.html')}")

    return success


def main():
    passed = run_checkpoint()
    if not passed:
        print("\nBlocking Gold promotion: Silver quality check failed.")
        sys.exit(1)
    print("\nSilver quality check passed. Proceeding to Gold.")
    sys.exit(0)


if __name__ == "__main__":
    main()
