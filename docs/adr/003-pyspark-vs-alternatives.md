# ADR-003: Apache Spark (PySpark) for Distributed Processing

**Status:** Accepted
**Date:** 2024-01-16
**Deciders:** Data Engineering

---

## Context

The Bronze → Silver layer must:

1. Read JSONL / CSV from object storage and apply schema enforcement at TB scale.
2. Deduplicate on a primary key idempotently — re-runs must not create duplicate rows.
3. Run a `MERGE` (upsert) of late-arriving job-completion events into an existing Delta table.
4. Run as both a daily batch job and a 30-second-micro-batch streaming job, ideally with the same engine and the same code patterns.
5. Submit cleanly from Airflow via a managed operator.

Single-node tools (pandas, Polars, DuckDB) cover the small-scale case but do not scale past a single host's memory. The platform's `node_metrics` dataset is already ~6.6 M rows for 90 days × 96 nodes; a real-cloud equivalent is orders of magnitude larger.

## Options considered

| Engine | Distributed | Delta MERGE | Streaming | Airflow operator | Skill availability |
|---|---|---|---|---|---|
| **Apache Spark (PySpark)** | ✅ | ✅ native via `delta-spark` | ✅ Structured Streaming | ✅ `SparkSubmitOperator` | Highest in DE market |
| Apache Flink | ✅ | ✅ via Iceberg sink | ✅ true event-streaming | ⚠️ via Beam runner | Lower; learning curve |
| Apache Beam | ✅ (multi-runner) | ⚠️ via Iceberg connector | ✅ | ⚠️ wrapper needed | Niche |
| Dask | ✅ | ❌ no Delta MERGE | ⚠️ Streamz, immature | ⚠️ no first-class operator | Smaller ecosystem |
| Ray Data | ✅ | ❌ no Delta MERGE | ⚠️ early | ⚠️ custom | Emerging |
| Single-node (Polars / DuckDB) | ❌ | ✅ DuckDB only | ❌ | n/a | Limits scale |

## Decision

**Apache Spark (PySpark) 3.5.x with `delta-spark` 3.1.0.**

### Why Spark over Flink

Flink is technically the better pure-streaming engine — true per-record processing, lower watermarking latency, more sophisticated state management. The platform does not need any of that:

- The streaming SLA is "30-second freshness for inference dashboards", not sub-second alerting.
- Spark Structured Streaming's micro-batch model (`processingTime="30 seconds"`) hits this comfortably.
- Using Spark for both batch and streaming means **one engine, one set of operational skills, one set of libraries** (`delta-spark`, `hadoop-aws`). Flink would require running two engines side-by-side.
- The MERGE pattern for late-arriving events sits naturally in Spark batch (`bronze_to_silver.py::merge_completions`). Flink would force it into a stateful streaming job, which is more complex than the problem requires.

### Why Spark over Dask / Ray

Dask and Ray Data are excellent for DataFrame-style distributed Python, but neither has a production-grade Delta Lake MERGE implementation. Without MERGE, the late-arrival pattern requires a hand-rolled read-modify-write that breaks the idempotency guarantee re-runs depend on.

### Why PySpark over Scala Spark

Code lives next to the rest of the Python codebase (generators, FastAPI, Airflow DAGs, tests). One language across the repo means one set of unit-test fixtures (`pytest` + `local[2]` SparkSession) and no JVM/Scala build pipeline. The performance penalty of PySpark's JVM bridge is invisible at this data scale; UDFs are kept SQL-shaped where possible.

## How it is used

| File | Pattern |
|---|---|
| [spark/jobs/bronze_to_silver.py](../../spark/jobs/bronze_to_silver.py) | Daily batch: schema enforcement, dedup, enrichment, Delta `MERGE` for late completions |
| [spark/jobs/streaming_consumer.py](../../spark/jobs/streaming_consumer.py) | Spark Structured Streaming: Kafka → Delta with 30 s micro-batches, `maxOffsetsPerTrigger=10000` back-pressure, 10-minute watermark |
| [spark/jobs/optimize_tables.py](../../spark/jobs/optimize_tables.py) | Nightly `OPTIMIZE … ZORDER BY` and `VACUUM RETAIN 168 HOURS` |
| [spark/utils/delta_utils.py](../../spark/utils/delta_utils.py) | Shared SparkSession builder pre-configured for Delta + S3A → MinIO; reusable `upsert_to_delta()` helper |
| [tests/unit/test_transformations.py](../../tests/unit/test_transformations.py) | Unit tests run in `local[2]` mode — no cluster needed in CI |
| [orchestration/dags/batch_pipeline_dag.py](../../orchestration/dags/batch_pipeline_dag.py) | `SparkSubmitOperator` submits to `spark://spark-master:7077` |

Cluster topology (development): one Spark master + 2 standalone workers via [Dockerfile.spark](../../Dockerfile.spark), 1 GB / 1 core each. Scaled with `make scale-spark N=4`.

Production: same code targets EMR Serverless / Databricks Jobs / Spark on K8s by changing `--master`. The S3A + Delta extensions and the `--packages` Maven coordinates do not change.

## Consequences

- Same engine spans batch and streaming — no Flink-style two-engine operational burden.
- Adaptive Query Execution and Kryo serializer are enabled by default in `delta_utils.get_spark_session()` for performance.
- Standalone cluster manager is the simplest production-equivalent for local dev. `SparkSubmitOperator` works identically against standalone, YARN, K8s, or EMR.
- The PySpark JVM bridge cost is acceptable; if it ever became a bottleneck, the highest-throughput jobs would migrate to Spark Connect or be rewritten in Scala without changing the surrounding pipeline.
- Spark UI on :8080 gives free per-job observability — no APM integration needed for development.
