# Deployment Guide

How to run the platform - locally, in Codespaces, on a free-tier cloud VM, or productionised on managed cloud services.

---

## Local (Docker Compose)

**Requirements:** Docker Desktop / Docker Engine + Compose plugin, Python 3.11 (only for `make up`'s key-generation step), 16 GB RAM recommended.

```bash
make up        # build images, start everything, scale workers to 2
make generate  # synthetic data - data/raw/
make ingest    # publish to Kafka + upload to MinIO bronze
make pipeline  # run batch pipeline manually (also runs daily at 01:00 UTC via Airflow)
```

`make up` performs first-time setup automatically:
- Copies `.env.example` - `.env` if missing
- Generates an Airflow Fernet key and webserver secret if placeholders remain
- Starts 2 Celery workers and 2 Spark workers
- Prints UI URLs

### Resource footprint (2-worker default)

| Component | CPU limit | Memory limit |
|---|---:|---:|
| MinIO | 1 | 1 GB |
| Redis | 0.5 | 512 MB |
| Zookeeper | 0.5 | 512 MB |
| Kafka × 3 | 1 each | 1 GB each |
| Postgres | 1 | 512 MB |
| Spark master | 2 | 2.5 GB |
| Spark worker × 2 | 1 each | 1 GB each |
| Airflow scheduler | 1 | 1 GB |
| Airflow webserver | 1 | 1 GB |
| Airflow worker × 2 | 2 each | **4 GB each** |
| Airflow triggerer | 0.5 | 512 MB |
| Flower | 0.5 | 512 MB |
| API | 1 | 512 MB |

The Airflow worker memory was bumped from 1 GB to 4 GB (commit `74762b4`) because the dbt Gold step aggregates 1.6 M-row datasets in DuckDB.

### Common operations

```bash
make ps                 # service status
make logs               # tail all logs
make logs-airflow-scheduler   # tail one service
make scale-workers N=4  # scale Celery workers without downtime
make scale-spark   N=4  # scale Spark workers
make down               # stop all services (preserve volumes)
make reset              # destroy everything including volumes (DESTRUCTIVE)
```

### Streaming demo

```bash
# Terminal 1 - Kafka - Delta consumer
make stream-start

# Terminal 2 - synthetic 100 req/s producer
make stream-live
```

The `streaming_health_check` Airflow DAG polls Kafka lag, Delta freshness, and SLO compliance every 15 minutes.

---

## GitHub Codespaces

[.devcontainer/devcontainer.json](../../.devcontainer/devcontainer.json) declares a 16 GB Codespace with all service ports forwarded. Workflow:

1. Fork the repo (or open the original) - "Code" - "Open in Codespace".
2. Wait for the container to build (~2 min).
3. `make up && make generate && make ingest && make pipeline`.
4. Click the forwarded port toasts to reach Airflow / API / MinIO UIs in the browser.

This is the path of least resistance for demos - zero local install, public-URL forwarding for sharing.

---

## Oracle Cloud Free Tier (always-free ARM)

OCI offers 4 ARM cores + 24 GB RAM + 200 GB block storage indefinitely. The platform fits comfortably with one worker each:

```bash
# On a fresh OCI Ampere A1 VM (Ubuntu 22.04 ARM)
sudo apt-get install -y docker.io docker-compose-plugin make python3.11
git clone <repo> && cd project-304-data-platform
make up
```

Open ports 8000, 8080, 8081, 8082, 9001 in the security list. All container images used (`apache/airflow`, `apache/spark`, `confluentinc/cp-kafka`, `minio/minio`, `redis`, `postgres`) ship multi-arch, so no source builds are needed.

---

## Production (cloud-native)

Compose is for development. Production swaps each component for a managed equivalent:

| Local component | Production replacement |
|---|---|
| MinIO | AWS S3 / GCS / Azure Blob Storage |
| Postgres container | RDS Postgres / Cloud SQL with point-in-time recovery |
| Redis container | ElastiCache / Memorystore |
| Kafka 3-broker | AWS MSK / Confluent Cloud / Aiven |
| Spark standalone | EMR Serverless / Databricks Jobs / Spark on K8s |
| Airflow on Compose | MWAA / Cloud Composer / Astronomer |
| FastAPI on Compose | ECS Fargate / Cloud Run / GKE |
| MinIO admin password in `.env` | AWS Secrets Manager / GCP Secret Manager / Vault |

### Storage swap (MinIO - S3)

Spark and DuckDB reach storage through environment variables - there is no code change required. Update `.env`:

```dotenv
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
MINIO_ENDPOINT=               # blank - use default AWS endpoint
DATA_BRONZE_PATH=s3a://your-bronze-bucket
DATA_SILVER_PATH=s3a://your-silver-bucket
DATA_GOLD_PATH=s3a://your-gold-bucket
DATA_CHECKPOINT_PATH=s3a://your-checkpoints-bucket
```

Remove the `spark.hadoop.fs.s3a.endpoint=http://minio:9000` and `path.style.access=true` configs from `bronze_to_silver.py` Spark submit args (or override via env). The S3A filesystem will pick up AWS credentials from the standard chain.

The Terraform in [infrastructure/terraform/](../../infrastructure/terraform/) currently uses the `aminueza/minio` provider. To target real S3, swap it for the official `hashicorp/aws` provider - the bucket resource shape is the same.

### Airflow on Kubernetes

Replace CeleryExecutor with KubernetesExecutor and run Airflow on a managed K8s cluster:
- Each task spawns a pod with its own resource request - no Celery worker pool to size.
- `SparkSubmitOperator` becomes `SparkKubernetesOperator` (spark-on-k8s-operator).
- Airflow metadata stays in RDS; logs ship to CloudWatch / Stackdriver via the `remote_logging` config.

### Spark on K8s / EMR

The PySpark jobs in [spark/jobs/](../../spark/jobs/) need no changes - only the `--master` argument shifts. `delta-spark`, `hadoop-aws`, and `aws-java-sdk-bundle` are passed via `--packages` either way.

For EMR Serverless, package the jobs into a wheel + uber-jar and submit via `aws emr-serverless start-job-run`. Spark's S3A filesystem already does the right thing without `spark.hadoop.fs.s3a.endpoint` overrides.

### Kafka on MSK

Update `KAFKA_BOOTSTRAP_SERVERS` to MSK's broker list. Switch the producer's `security.protocol` to `SASL_SSL` and add IAM authentication callbacks. Topic configuration (RF=3, min-isr=2, retention=7d, partitions=6) translates directly via `--config` flags or Terraform's `aws_msk_cluster` resource.

### dbt-duckdb at scale

dbt-duckdb's single-thread limitation in prod (DuckDB's `delta` extension is not thread-safe) is acceptable up to perhaps 10 GB of Gold output. Beyond that, switch to dbt-spark or dbt-databricks: the SQL in the staging / intermediate / mart models is ANSI-compatible and should port with minor adjustments to `PERCENTILE_CONT` and `EXTRACT EPOCH` syntax.

---

## CI/CD

[.github/workflows/ci.yml](../../.github/workflows/ci.yml) runs five parallel jobs on every push and pull request:

| Job | Purpose |
|---|---|
| `lint` | ruff over `spark/`, `ingestion/`, `data/`, `serving/`, `quality/`, `orchestration/` |
| `unit-tests` | PySpark in `local[2]` mode against [tests/unit/](../../tests/unit/) |
| `dbt-compile` | `dbt compile` validates SQL with no data needed |
| `gx-suite-syntax` | Imports the GX suite to catch syntax issues |
| `docker-build` | Builds the API and Airflow images |

A sixth job - `integration` - runs only on push to `main`, brings up MinIO + a single Kafka broker via `docker compose`, and runs a smoke test.

---

## Observability

| Tool | URL | What it shows |
|---|---|---|
| Airflow UI | :8081 | DAG runs, task logs, XCom, manual triggers |
| Flower | :5555 | Celery worker state, task throughput |
| Spark Master UI | :8080 | Active applications, worker registration, completed jobs |
| Kafka UI | :8082 | Topics, partitions, consumer group lag, message browser |
| MinIO Console | :9001 | Bucket browser, object versions, lifecycle policies |
| FastAPI Swagger | :8000/docs | Interactive endpoint docs |

There is no Prometheus / Grafana stack wired in - that is the natural next addition for production. MinIO exposes `MINIO_PROMETHEUS_AUTH_TYPE=public` so a sidecar Prometheus could scrape it without authentication.
