# Project 304: Data Platform for AI — Executive Summary

**Duration**: 85 hours · **Complexity**: Very High · **Project**: project-304-data-platform

## One-line pitch

A unified **lakehouse data platform** for **TechCorp's** GPU cloud — replacing 3 disconnected telemetry stores and a 5-day manual billing close with **automated batch + streaming pipelines, quality-gated promotion, and self-serve REST APIs**.

## The problem

| Pain | Today | What it costs TechCorp |
|---|---|---|
| 3 disconnected telemetry stores | Postgres (jobs), S3 (inference), InfluxDB (nodes) | No single source of truth for `$ × utilisation × SLO` |
| Manual monthly billing close | 5 days, ±8% reconciliation error | $1.2M missed last quarter; board lost confidence |
| SLO breaches reported by customers | 6 h average detection lag | Two large customers cited in renewal pushback |
| Data scientists blocked on tickets | 4-day wait per cost cut | 12 DS blocked by 4-person DE queue |
| No automated quality gate | Bad GPU price made it to billing | Silent failures reach Gold ~2× per quarter |

## The solution

| Capability | Implementation | Outcome |
|---|---|---|
| **100K+ events/day batch** | Spark + Delta Lake medallion (Bronze → Silver → Gold) | One queryable surface for cost & utilisation |
| **100 req/s streaming** | Spark Structured Streaming + Kafka, 30 s micro-batches, exactly-once | < 60 s end-to-end lag on inference SLO |
| **99.9% data quality** | Great Expectations gate + dbt tests + custom singular tests | Bad data **provably** blocked from Gold |
| **50% reduction in DE time** | Self-serve REST API + dbt + DuckDB | 4 days → 5 min time-to-first-insight |
| **End-to-end lineage** | dbt DAG + Delta time-travel + manual trace | GDPR Art. 30 + SOC 2 Processing Integrity ready |

## Business value

- **Productivity** — Data scientists self-serve via `/cost/*`, `/sla/*`, `/utilization/*` endpoints; no more DE ticket queue.
- **Quality** — 9-expectation GX checkpoint runs between Silver and Gold; failed checks branch the DAG away from `dbt run` and Gold is **not** written.
- **Compliance** — Complete lineage trace + Delta 7-day time travel + structural readiness for SOC 2 Type II and GDPR Art. 30.
- **Cost** — Lakehouse pattern projected ~60% cheaper than the alternative (separate lake + warehouse) at TechCorp scale.

## Architecture at a glance

```
Kafka (RF=3) ──┐
               ├─→ Bronze ──→ Silver ──→ GX gate ──→ Gold ──→ FastAPI
batch CSV ─────┘  (MinIO/   (Delta    (block on    (dbt +    (/cost
                  Delta)    Lake +    failure)    DuckDB)    /sla
                            MERGE)                            /utilization)
                                       Airflow CeleryExecutor orchestrates
```

## Key architecture decisions

| Decision | Choice | Why |
|---|---|---|
| **Lakehouse format** | **Delta Lake** | ACID + MERGE for late-arriving events; native DuckDB reads; widest enterprise adoption ([ADR-001](docs/architecture-decisions/adr/001-delta-lake-vs-parquet.md)) |
| **Streaming platform** | **Kafka** + Spark Structured Streaming | Battle-tested durability (RF=3, min.isr=2); 30 s micro-batch fits SLA; exactly-once via offsets + Delta tx log ([ADR-003](docs/architecture-decisions/adr/003-pyspark-vs-alternatives.md)) |
| **Orchestration** | **Airflow** CeleryExecutor | `SparkSubmitOperator` ecosystem; horizontal worker scaling ([ADR-002](docs/architecture-decisions/adr/002-airflow-vs-prefect.md)) |
| **Governance** | 3-checkpoint quality gate (Bronze schema-on-read → Silver GX gate → Gold dbt tests) | Bad data fails closed before billing ([GOVERNANCE.md](docs/governance/GOVERNANCE.md)) |
| **IaC** | **Terraform** (same .tf for MinIO + S3) | Bucket policies + lifecycle as code ([ADR-004](docs/architecture-decisions/adr/004-terraform-for-iac.md)) |
| **Dev → Prod** | **Docker Compose** locally, **Kubernetes** in prod | Same images both targets; no re-architecture for migration ([ADR-005](docs/architecture-decisions/adr/005-docker-compose-dev-k8s-prod.md)) |

## Investment & ROI

| Metric | Value |
|---|---|
| Capital | **$2.4M** one-time |
| Annual operating | **$1.8M** |
| Payback period | **Month 18** |
| 2-year ROI | **220%** |
| 3-year NPV | **~$2.7M** (10% discount, conservative 3-year view) |

Full case in [docs/business/BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md).

## What to read next

| Audience | Document | Time |
|---|---|---|
| **Executives** | [docs/business/BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md), [docs/business/PRESENTATION.md](docs/business/PRESENTATION.md) | 10 min |
| **Architects / VP Eng** | [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md), [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/) | 30 min |
| **CISO / DPO** | [docs/governance/SECURITY.md](docs/governance/SECURITY.md), [docs/governance/GOVERNANCE.md](docs/governance/GOVERNANCE.md) | 20 min |
| **Data Scientists / Engineers** | [README.md](README.md), [docs/architecture/API.md](docs/architecture/API.md), [docs/operations/STEP_BY_STEP.md](docs/operations/STEP_BY_STEP.md) | 15 min |
| **Finance** | [docs/business/COST-MODEL.md](docs/business/COST-MODEL.md), [docs/business/BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md) | 15 min |

See [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) for the complete design.
