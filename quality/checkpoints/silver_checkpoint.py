"""
Great Expectations checkpoint for the Silver layer.

Loads silver/jobs from MinIO via DuckDB's delta extension, then runs the
silver_jobs suite against an in-memory ephemeral GX context. No project
files needed — fits the read-only `/opt/quality` mount.

Exit 0 = passed, exit 1 = failed (Airflow marks the task as failed,
blocking Gold promotion in the batch DAG).
"""

import os
import sys

import duckdb
import great_expectations as gx
import pandas as pd

sys.path.insert(0, "/opt/quality")
from expectations.suite_silver_jobs import apply_silver_jobs_expectations

# DuckDB's httpfs uses host:port (no scheme); s3:// path form (not s3a://)
SILVER_PATH = os.getenv("DATA_SILVER_PATH", "s3a://silver").replace("s3a://", "s3://")
MINIO_HOST = (
    os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    .replace("http://", "")
    .replace("https://", "")
)
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")


def load_silver_jobs() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta;  LOAD delta;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_HOST}';
        SET s3_access_key_id='{AWS_KEY}';
        SET s3_secret_access_key='{AWS_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    df = con.execute(f"""
        SELECT job_id, org_id, user_id, gpu_type, gpu_count,
               framework, model_arch, started_at, ended_at,
               cost_usd, exit_code, is_success, is_late_arrival
        FROM delta_scan('{SILVER_PATH}/jobs')
        LIMIT 100000
    """).df()
    con.close()
    print(f"Loaded {len(df):,} rows from {SILVER_PATH}/jobs")
    return df


def main() -> int:
    df = load_silver_jobs()

    context = gx.get_context()  # ephemeral, in-memory
    asset = (
        context.sources.add_pandas("silver_pandas")
        .add_dataframe_asset("silver_jobs")
    )
    suite = context.add_or_update_expectation_suite("silver_jobs.warning")
    validator = context.get_validator(
        batch_request=asset.build_batch_request(dataframe=df),
        expectation_suite=suite,
    )

    apply_silver_jobs_expectations(validator)
    results = validator.validate()

    passed = sum(1 for r in results.results if r.success)
    failed = sum(1 for r in results.results if not r.success)
    print("\n" + "=" * 60)
    print(f"GX: {passed} passed, {failed} failed — {'PASS' if results.success else 'FAIL'}")
    print("=" * 60)

    if not results.success:
        for r in results.results:
            if not r.success:
                print(f"  FAIL  {r.expectation_config.expectation_type}")
                print(f"        {dict(r.result)}")
        print("\nBlocking Gold promotion: Silver quality check failed.")
        return 1

    print("\nSilver quality check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
