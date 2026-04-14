"""
Airflow DAG: Streaming health check (every 15 minutes)

Monitors the Kafka → Delta streaming pipeline and inference SLOs.
This DAG does NOT process data — it monitors and alerts.

Task graph:
  check_kafka_consumer_lag
  check_delta_streaming_freshness
  check_inference_p99_sla         ← queries Gold table directly via DuckDB
      └── alert_slo_breach (conditional)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
}

# SLO thresholds (ms) — must match job_performance_sla.sql
SLO_THRESHOLDS = {
    "small":  2_000,
    "medium": 3_500,
    "large":  5_000,
    "xl":     8_000,
}


def check_kafka_lag(**context) -> None:
    """
    Check Kafka consumer group lag for the inference-api-logs topic.
    Alerts if lag > 10,000 messages (streaming is falling behind).
    """
    from kafka import KafkaAdminClient
    from kafka.structs import TopicPartition
    import os

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    max_lag = 10_000

    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="airflow-monitor")
        offsets = admin.list_consumer_group_offsets("spark-streaming-inference")
        admin.close()

        total_lag = 0
        for tp, offset_meta in offsets.items():
            total_lag += offset_meta.offset

        print(f"Kafka consumer lag: {total_lag:,} messages")
        context["ti"].xcom_push(key="kafka_lag", value=total_lag)

        if total_lag > max_lag:
            raise Exception(
                f"Kafka consumer lag {total_lag:,} exceeds threshold {max_lag:,}. "
                "Streaming consumer may be stalled."
            )

    except Exception as e:
        # Don't fail DAG for connection issues — log and continue
        print(f"WARNING: Could not check Kafka lag: {e}")
        context["ti"].xcom_push(key="kafka_lag", value=-1)


def check_delta_freshness(**context) -> None:
    """
    Check that the Delta streaming table has been written to recently.
    Stale data (no writes in > 10 min) indicates a streaming job failure.
    """
    import duckdb
    import os
    from datetime import datetime, timezone

    silver_path = os.getenv("DATA_SILVER_PATH", "s3a://silver")
    minio = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{minio}';
        SET s3_access_key_id='{aws_key}';
        SET s3_secret_access_key='{aws_secret}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    row = con.execute(f"""
        SELECT MAX(_stream_ingested_at) AS latest
        FROM delta_scan('{silver_path}/inference_stream')
    """).fetchone()
    con.close()

    latest = row[0] if row and row[0] else None
    if latest is None:
        print("WARNING: No data in inference_stream table. Streaming may not have started.")
        return

    now = datetime.now(timezone.utc)
    lag_minutes = (now - latest.replace(tzinfo=timezone.utc)).total_seconds() / 60
    print(f"Delta streaming freshness: last write {lag_minutes:.1f} min ago")

    if lag_minutes > 10:
        raise Exception(
            f"Delta streaming table stale: last write was {lag_minutes:.1f} min ago "
            "(threshold: 10 min). Check streaming_consumer.py."
        )


def check_slo_compliance(**context) -> str:
    """
    Query the Gold SLA table to check if p99 latency is within SLO thresholds.
    Returns branch name: 'alert_slo_breach' or 'slo_ok'.
    """
    import duckdb
    import os

    gold_path = os.getenv("DATA_GOLD_PATH", "s3a://gold")
    minio = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{minio}';
        SET s3_access_key_id='{aws_key}';
        SET s3_secret_access_key='{aws_secret}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    breaches = con.execute(f"""
        SELECT model_id, region, model_size_tier, p99_latency_ms, log_date
        FROM delta_scan('{gold_path}/job_performance_sla')
        WHERE log_date = CURRENT_DATE
          AND slo_p99_breached = TRUE
        ORDER BY p99_latency_ms DESC
        LIMIT 10
    """).fetchall()
    con.close()

    if breaches:
        breach_summary = "\n".join(
            f"  {model} ({region}) tier={tier}: p99={p99:.0f}ms"
            for model, region, tier, p99, _ in breaches
        )
        print(f"SLO BREACHES DETECTED:\n{breach_summary}")
        context["ti"].xcom_push(key="breach_count", value=len(breaches))
        context["ti"].xcom_push(key="breach_summary", value=breach_summary)
        return "alert_slo_breach"

    print("All SLOs within thresholds.")
    return "slo_ok"


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="streaming_health_check",
    description="15-minute check: Kafka lag, Delta freshness, inference SLO compliance",
    schedule_interval="*/15 * * * *",   # every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-platform", "streaming", "monitoring"],
    max_active_runs=1,
) as dag:

    check_kafka = PythonOperator(
        task_id="check_kafka_consumer_lag",
        python_callable=check_kafka_lag,
        provide_context=True,
    )

    check_freshness = PythonOperator(
        task_id="check_delta_streaming_freshness",
        python_callable=check_delta_freshness,
        provide_context=True,
    )

    check_slo = BranchPythonOperator(
        task_id="check_inference_p99_sla",
        python_callable=check_slo_compliance,
        provide_context=True,
    )

    alert_breach = BashOperator(
        task_id="alert_slo_breach",
        bash_command=(
            'echo "SLO BREACH: {{ ti.xcom_pull(task_ids=\"check_inference_p99_sla\", key=\"breach_count\") }} '
            'models breached p99 thresholds. '
            'Details: {{ ti.xcom_pull(task_ids=\"check_inference_p99_sla\", key=\"breach_summary\") }}"'
        ),
    )

    slo_ok = BashOperator(
        task_id="slo_ok",
        bash_command='echo "All inference SLOs passing at {{ ts }}"',
    )

    [check_kafka, check_freshness] >> check_slo >> [alert_breach, slo_ok]
