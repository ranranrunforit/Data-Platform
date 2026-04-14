# ADR-002: Airflow over Prefect/Dagster for Orchestration

**Status:** Accepted  
**Date:** 2024-01-15  
**Deciders:** Data Engineering

---

## Context

The pipeline requires an orchestrator that can:
1. Schedule the daily batch job with retry logic and alerting.
2. Run the 15-minute streaming health check with branching (SLO breach → alert path).
3. Submit Spark jobs to the standalone cluster via `SparkSubmitOperator`.
4. Be widely understood by future team members without a learning curve.

## Options Considered

### Apache Airflow 2.8
- **Pros:** Industry standard; widest operator library (SparkSubmitOperator, FileSensor, etc.); every DE team knows it; explicit DAG-as-code model is auditable.
- **Cons:** Complex setup; heavyweight; UI is dated; scheduler can lag under heavy load.

### Prefect 2.x
- **Pros:** Pythonic; simpler setup; good local dev story; hybrid execution model.
- **Cons:** Spark integration requires custom code (no SparkSubmitOperator equivalent); smaller ecosystem; less familiar to interviewers.

### Dagster
- **Pros:** Asset-based model is conceptually cleaner; excellent observability; type-checked.
- **Cons:** Steepest learning curve; overkill for this pipeline complexity; Spark integration is immature.

## Decision

**Apache Airflow.**

The primary reason is the JD: it explicitly lists Airflow as the orchestration tool example. Using Airflow directly maps the project to the job requirement.

Secondary reason: the `SparkSubmitOperator` with Spark connection configuration is a common real-world pattern. Demonstrating it shows operational familiarity, not just Python scripting.

The complexity cost of Airflow (postgres backend, worker setup, init container) is acceptable because Docker Compose abstracts it. The `make up` command handles everything.

## What We'd Change at Scale

At 100+ DAGs and 50+ engineers, we'd evaluate Dagster for its asset-lineage model. The Software-Defined Assets paradigm maps more cleanly to a medallion architecture than Airflow's task-graph model. This is a known limitation we'd address at scale.

## Consequences

- Two DAGs: `batch_pipeline_daily` (01:00 UTC) and `streaming_health_check` (*/15 * * * *).
- SparkSubmitOperator submits to `spark://spark-master:7077`.
- GX checkpoint is a PythonOperator task that blocks Gold promotion on failure.
- Airflow metadata lives in Postgres (separate from data storage).
