"""
Airflow DAG: Daily batch pipeline (Bronze → Silver → Gold)

Schedule: 01:00 UTC daily (processes previous day's data)

Task graph:
  sense_bronze_data
      └── spark_bronze_to_silver
              └── gx_silver_quality_check   ← BLOCKS Gold on failure
                      └── dbt_silver_to_gold
                              └── dbt_test_gold
                                      └── optimize_delta_tables
                                              └── notify_success

On GX failure: notify_quality_failure (does NOT proceed to Gold)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.1.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "8",
}

DBT_CMD = (
    "dbt {command} "
    "--project-dir /opt/dbt "
    "--profiles-dir /opt/dbt "
    "--target prod "
    "--no-use-colors"
)


# ── Quality check branch ──────────────────────────────────────────────────────
def branch_on_quality(**context) -> str:
    """
    Read the GX result from XCom and branch:
      - quality passed → proceed to dbt
      - quality failed → notify and stop (do NOT write bad data to Gold)
    """
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="gx_silver_quality_check", key="gx_passed")
    if result:
        return "dbt_silver_to_gold"
    return "notify_quality_failure"


def run_gx_checkpoint(**context) -> None:
    """Run the GX silver checkpoint and push pass/fail to XCom."""
    import subprocess
    result = subprocess.run(
        ["python", "/opt/quality/checkpoints/silver_checkpoint.py"],
        capture_output=True,
        text=True,
    )
    passed = result.returncode == 0
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    context["ti"].xcom_push(key="gx_passed", value=passed)

    if not passed:
        print("GX validation FAILED. Gold promotion blocked.")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="batch_pipeline_daily",
    description="Bronze → Silver (Spark MERGE) → GX quality gate → Gold (dbt)",
    schedule_interval="0 1 * * *",   # 01:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-platform", "batch", "production"],
    max_active_runs=1,
) as dag:

    # ── 1. Sense that bronze data landed ─────────────────────────────────
    # In production this would be an S3KeySensor or Kafka lag sensor.
    # For dev, it checks for the generated JSONL files.
    sense_bronze_data = BashOperator(
        task_id="sense_bronze_data",
        bash_command=(
            "python -c \""
            "import boto3, sys; "
            "s3 = boto3.client('s3', endpoint_url='{{ var.value.minio_endpoint }}', "
            "  aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123'); "
            "objs = s3.list_objects_v2(Bucket='bronze', Prefix='job_events/'); "
            "count = objs.get('KeyCount', 0); "
            "print(f'Bronze objects: {count}'); "
            "sys.exit(0 if count > 0 else 1)"
            "\""
        ),
        retries=6,
        retry_delay=timedelta(minutes=10),
    )

    # ── 2. Spark: Bronze → Silver ─────────────────────────────────────────
    spark_bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/spark-apps/jobs/bronze_to_silver.py",
        conn_id="spark_default",                 # set up by airflow-init
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        application_args=[],
        name="bronze_to_silver_{{ ds }}",
        verbose=False,
        execution_timeout=timedelta(hours=1),
    )

    # ── 3. Great Expectations quality gate ───────────────────────────────
    gx_silver_quality_check = PythonOperator(
        task_id="gx_silver_quality_check",
        python_callable=run_gx_checkpoint,
        provide_context=True,
    )

    # ── 4. Branch: pass → Gold, fail → alert ─────────────────────────────
    quality_branch = BranchPythonOperator(
        task_id="quality_branch",
        python_callable=branch_on_quality,
        provide_context=True,
    )

    # ── 5a. dbt: Silver → Gold ────────────────────────────────────────────
    dbt_silver_to_gold = BashOperator(
        task_id="dbt_silver_to_gold",
        bash_command=DBT_CMD.format(command="run"),
    )

    # ── 5b. dbt tests ─────────────────────────────────────────────────────
    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=DBT_CMD.format(command="test"),
    )

    # ── 6. OPTIMIZE Delta tables (Z-ORDER for fast reads) ─────────────────
    optimize_delta_tables = SparkSubmitOperator(
        task_id="optimize_delta_tables",
        application="/opt/spark-apps/jobs/optimize_tables.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        name="optimize_tables_{{ ds }}",
        verbose=False,
    )

    # ── 7a. Success notification ──────────────────────────────────────────
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command=(
            'echo "Pipeline complete for {{ ds }}. '
            'Silver rows processed, Gold models updated."'
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 7b. Quality failure notification ─────────────────────────────────
    notify_quality_failure = BashOperator(
        task_id="notify_quality_failure",
        bash_command=(
            'echo "ALERT: Silver quality check FAILED for {{ ds }}. '
            'Gold NOT updated. Investigate GX Data Docs."'
        ),
    )

    # ── Task dependencies ─────────────────────────────────────────────────
    (
        sense_bronze_data
        >> spark_bronze_to_silver
        >> gx_silver_quality_check
        >> quality_branch
        >> [dbt_silver_to_gold, notify_quality_failure]
    )
    dbt_silver_to_gold >> dbt_test_gold >> optimize_delta_tables >> notify_success
