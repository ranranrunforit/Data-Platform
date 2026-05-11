# Step-by-Step Walkthrough

A first-run playbook for someone who just cloned the repo and wants to see the platform end-to-end. About 15 minutes start to finish (after the first-time image build).

---

## Prerequisites

- Docker Desktop (Mac / Windows) or Docker Engine + Compose plugin (Linux)
- Python 3.11 on the host (only used by `make up` to generate Airflow secrets)
- 16 GB RAM recommended
- ~10 GB free disk for images and volumes

---

## Step 1 â€?Bring up the stack

```bash
make up
```

What happens:

1. `.env.example` is copied to `.env` if missing.
2. `make up` checks for placeholder values in `.env`; if found, generates a Fernet key and webserver secret with `python -c "from cryptography.fernet import Fernet; â€?`.
3. `docker compose up -d --build --scale airflow-worker=2 --scale spark-worker=2` builds two custom images and pulls the rest:
   - `Dockerfile.airflow` â€?Airflow 2.10.4 + JDK 17 + Spark 3.5.1 client
   - `Dockerfile.spark` â€?Spark 3.5.4 + delta-spark 3.1.0
4. `airflow-init` runs `airflow db migrate`, creates the `admin/admin` user, and registers the `spark_default` and `aws_default` connections.
5. `minio-init` creates buckets `bronze`, `silver`, `gold`, `checkpoints` and applies a 90-day expiry rule on bronze.
6. `kafka-init` creates topics `gpu-job-events`, `gpu-job-completions`, `inference-api-logs` with RF=3, 6 partitions, 7-day retention.

First run: 8â€?0 minutes (image pulls + Spark JAR downloads). Subsequent runs: ~60 seconds.

Verify everything is up:

```bash
make ps
```

You should see ~14 services in `healthy` or `running` state.

---

## Step 2 â€?Generate synthetic data

```bash
make generate
```

Runs three Python scripts in sequence:

- `data/generator/job_events.py` â†?50 000 GPU job records + ~7 500 late-completion events
- `data/generator/inference_logs.py` â†?500 000 inference requests
- `data/generator/node_metrics.py` â†?~6.6 M node metric rows (96 nodes Ã— 8 GPUs Ã— hourly Ã— 90 days)

Output lands in `data/raw/`. Total size â‰?1.3 GB (the node metrics CSV is the bulk).

---

## Step 3 â€?Ingest

```bash
make ingest
```

Two things happen:

1. `kafka-init-topics` re-applies topic configuration (idempotent).
2. `upload_to_bronze.py` uploads every JSONL / CSV file from `data/raw/` to MinIO `bronze/`.
3. `job_producer.py` reads `data/raw/job_events/*.jsonl` and publishes 50 000 + 7 500 records to Kafka.
4. `inference_producer.py` reads `data/raw/inference_logs/*.jsonl` and publishes 500 000 records to Kafka.

You can browse the topics in **Kafka UI** at http://localhost:8082 â€?pick a topic, click **Messages**, and see the JSON payloads.

You can browse the bronze bucket in **MinIO Console** at http://localhost:9001 (login `minioadmin` / `minioadmin123`).

---

## Step 4 â€?Run the batch pipeline

```bash
make pipeline
```

This is the same pipeline that the `batch_pipeline_daily` Airflow DAG runs every night at 01:00 UTC, but invoked manually so you can see its output:

1. **Spark Bronze â†?Silver** â€?`spark-submit` runs `bronze_to_silver.py`. Watch logs scroll past as it reads JSONL, dedupes, enriches, MERGEs late completions, and writes Delta tables to `silver/`.
2. **GX quality gate** â€?`silver_checkpoint.py` loads up to 100K Silver rows via DuckDB into pandas and runs every expectation. Output ends with `GX: N passed, M failed â€?PASS` (or FAIL).
3. **dbt Silver â†?Gold** â€?`dbt run --target prod` builds:
   - `stg_jobs`, `stg_inference` (views)
   - `int_job_costs` (table)
   - `cost_attribution`, `gpu_utilization_hourly`, `job_performance_sla` (tables)

Total runtime: 1â€? minutes depending on host.

Verify Gold is populated:

```bash
docker compose exec airflow-worker bash -c "
  python -c \"
import duckdb
con = duckdb.connect()
con.execute(\\\"INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;\\\")
con.execute(\\\"CREATE OR REPLACE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin123', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false);\\\")
print(con.execute(\\\"SELECT COUNT(*) FROM delta_scan('s3://gold/cost_attribution')\\\").fetchone())
\""
```

---

## Step 5 â€?Hit the API

```bash
curl -s http://localhost:8000/health
# {"status":"ok"}

curl -s http://localhost:8000/metrics/summary | jq
curl -s "http://localhost:8000/cost/orgs?days=7" | jq '.orgs[:3]'
curl -s "http://localhost:8000/sla/models?breaches_only=true" | jq
```

Or open the Swagger UI at http://localhost:8000/docs and click **Try it out** on any endpoint.

---

## Step 6 â€?Trigger the streaming path

In one terminal:

```bash
make stream-start
```

This submits `streaming_consumer.py` to Spark in foreground mode. The first batch takes ~30 seconds to initialise; subsequent batches process every 30 seconds.

In a second terminal:

```bash
make stream-live
```

This runs `inference_producer.py --live --rate 100` â€?synthetic inference requests at 100 req/s with real-time timestamps.

Within ~60 seconds you should see Spark's micro-batch logs reporting rows being written to `s3a://bronze/inference_stream/`. Browse MinIO Console to see new Delta files appearing under `bronze/inference_stream/`.

The `streaming_health_check` Airflow DAG (every 15 minutes) will:
- Check Kafka consumer group lag for `spark-streaming-inference`
- Query `MAX(_stream_ingested_at)` on the streaming Delta table â€?alerts if > 10 min old
- Query today's `gold/job_performance_sla` rows for `slo_p99_breached = TRUE`

Watch the DAG live: http://localhost:8081 â†?DAGs â†?`streaming_health_check`.

---

## Step 7 â€?Explore the Airflow DAGs

http://localhost:8081 (admin / admin)

- **`batch_pipeline_daily`** â€?manually trigger via the play button. The graph view shows the quality gate's branching: success path goes to `dbt_silver_to_gold`, failure path goes to `notify_quality_failure`. Click any task â†?**Logs** to see exactly what ran.
- **`streaming_health_check`** â€?every 15 minutes. Tasks `check_kafka_consumer_lag` and `check_delta_streaming_freshness` run in parallel, then `check_inference_p99_sla` branches to `alert_slo_breach` or `slo_ok`.

http://localhost:5555 (Flower) shows the live Celery worker pool. Scale workers and watch them appear:

```bash
make scale-workers N=4
```

---

## Step 8 â€?Inspect Delta tables

The MinIO Console makes the medallion structure visible:

```
bronze/
â”œâ”€â”€ job_events/         job_events_20240115.jsonl
â”œâ”€â”€ job_completions/    job_completions_20240115.jsonl
â”œâ”€â”€ inference_logs/     inference_logs_20240115.jsonl
â”œâ”€â”€ inference_stream/   _delta_log/  log_date=â€?  (after streaming runs)
â””â”€â”€ node_metrics/       node_metrics_20240115.csv

silver/
â”œâ”€â”€ jobs/               _delta_log/  job_date=â€?
â”œâ”€â”€ inference/          _delta_log/  log_date=â€?
â””â”€â”€ node_metrics/       _delta_log/  metric_date=â€?

gold/
â”œâ”€â”€ cost_attribution/        _delta_log/  *.parquet
â”œâ”€â”€ gpu_utilization_hourly/  _delta_log/  *.parquet
â””â”€â”€ job_performance_sla/     _delta_log/  *.parquet

checkpoints/
â””â”€â”€ inference_stream/   commits/  offsets/  sources/  state/
```

Open any `_delta_log/00000000000000000000.json` file to see Delta's transaction log entry â€?schema, add/remove file actions, transaction IDs.

---

## Step 9 â€?Run the test suite

```bash
make ci
```

Runs in this order:
1. **`make lint`** â€?ruff over `spark/`, `ingestion/`, `data/`, `serving/`, `quality/`, `orchestration/`, `tests/`
2. **`make test`** â€?pytest against `tests/unit/test_transformations.py` (PySpark local mode, no cluster)
3. **`make dbt-test`** â€?schema + custom dbt tests against the Gold tables
4. **`make gx-validate`** â€?runs the GX checkpoint standalone

---

## Step 10 â€?Tear down (or keep running)

Preserve volumes (recommended â€?re-running `make up` is fast):

```bash
make down
```

Destroy everything including data (DESTRUCTIVE):

```bash
make reset
```

---

## What to read next

- [ARCHITECTURE.md](../architecture/ARCHITECTURE.md) â€?full system design and storage layout
- [DATA-MODEL.md](../architecture/DATA-MODEL.md) â€?every column in every table
- [API.md](../architecture/API.md) â€?REST endpoint reference
- [COST-MODEL.md](../business/COST-MODEL.md) â€?billing logic
- [GOVERNANCE.md](../governance/GOVERNANCE.md) â€?quality, lineage, ownership
- [SECURITY.md](../governance/SECURITY.md) â€?what's missing for production
- [DEPLOYMENT.md](DEPLOYMENT.md) â€?moving off Docker Compose
