# Data Platform for AI - Detailed Requirements

> Concrete instantiation of the project-304 requirements template against the implementation in this repository. Every requirement below maps to actual code, a configuration, or a documented decision.

---

## Executive summary

This document specifies the functional, non-functional, and operational requirements for **TechCorp's unified AI infrastructure data platform**. The solution ingests batch and streaming telemetry from a 96-node GPU cluster, gates promotion on automated quality checks, and serves business-ready marts via REST API. Requirements address business needs (cost transparency, SLO observability, self-serve analytics) while meeting technical (lakehouse, exactly-once streaming, MERGE for late arrivals), security (gap-analysed dev - prod), and compliance (SOC 2 + GDPR structural readiness) requirements.

Full implementation: see [README.md](README.md). Architecture: see [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md).

---

## Business context

### Company overview

**TechCorp** is a Fortune-500 GPU cloud provider:
- **Industry**: AI infrastructure / cloud computing
- **Size**: ~5,000 employees globally; ~150 customer organisations
- **Revenue**: ~$2.8B annually (target $5B by year three of this platform's life)
- **ML maturity**: Mid-to-late stage - internal teams ship models; customers train and serve their own. Lacks unified observability across that estate.

### Business drivers

1. **Cost transparency (revenue protection)** - Manual monthly billing exports caused a $1.2M reconciliation miss last quarter; finance has lost board confidence in the GPU-cost line.
2. **SLO credibility (customer retention)** - p99 latency breaches are reported by customers before TechCorp's own dashboards notice. Two large customers cited this in renewal negotiations.
3. **Self-serve analytics (productivity)** - 12 data scientists are blocked behind a 4-person data-engineering ticket queue. Average wait for a new cost cut is 4 days.
4. **Compliance (regulatory)** - SOC 2 Type II is a non-negotiable for the federal vertical TechCorp wants to enter in 2027. GDPR Art. 30 (records of processing) is needed for the EU expansion already in flight.

### Success metrics

| Metric | Baseline | 6-month target | 12-month target |
|---|---|---|---|
| Time-to-first-insight (data scientist) | 4 days | 30 minutes | 5 minutes |
| Monthly billing close | 5 days | 1 day | Real-time (continuous) |
| Billing reconciliation error | ±8% | ±2% | ±0.5% |
| SLO breach detection lag (customer-reported vs. platform-detected) | 6 h (customer first) | < 5 min (platform first) | < 30 s (platform first) |
| Data-quality incidents reaching Gold | ~2 per quarter (silent failures) | 0 (GX gate blocks) | 0 |
| Data-engineering ticket backlog | ~40 open | < 10 open | < 5 open |
| **ROI**: payback period | n/a | on track for month-18 breakeven | 220% by year 2 - see [BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md) |

---

## Stakeholder analysis

### Key stakeholders

| Stakeholder | Role | Concerns | Requirements driven |
|---|---|---|---|
| **CTO** | Executive sponsor | Strategic alignment, ROI, build-vs-buy posture | FR-1, FR-3, FR-7 (multi-cloud), NFR-COST-1 |
| **VP Engineering** | Technical owner | Reliability, scalability, on-call load | NFR-A-1/2/3, FR-7, FR-9 |
| **CISO** | Security & compliance | SOC 2, GDPR, audit trail | NFR-SEC-1/2/3/4, FR-8, FR-9 |
| **CFO / Finance Director** | Budget owner & customer of cost mart | TCO, billing accuracy, audit-readiness | FR-4, NFR-COST-1/2/3, FR-3 |
| **Head of Customer Success** | Customer of SLO mart | Renewal risk, breach response | FR-5, FR-6 (`/sla/*` endpoints) |
| **Data Science lead** | Primary user | Latency to insight, self-serve | FR-6, NFR-U-1, NFR-P-1 |
| **Capacity Planning** | Customer of utilisation mart | Fleet utilisation, hardware mix | FR-1 (node metrics), FR-6 (`/utilization/*`) |
| **Platform Engineering** | Operator | Deploy / scale / debug | NFR-U-2, FR-7, FR-10 (IaC) |
| **DPO (Data Protection Officer)** | Regulatory | GDPR Arts. 5, 17, 30, 32 | FR-8, FR-9, NFR-SEC-4 |

Detailed audience-specific messaging and communication cadence: [docs/stakeholders/STAKEHOLDERS.md](docs/stakeholders/STAKEHOLDERS.md).

### Communication plan

| Audience | Cadence | Channel | Owner |
|---|---|---|---|
| Executive committee (CTO, CFO, CISO) | Monthly | 30-min review + dashboard | Architect |
| Engineering teams | Weekly | Sprint review (Mon), tech sync (Thu) | Eng manager |
| Security team | Bi-weekly | Pair review of new endpoints / DAGs | Architect + CISO rep |
| Finance | Monthly | Billing-mart walkthrough + variance review | Architect + Finance Director |
| Customer success | Weekly during MVP, monthly after GA | SLO dashboard demo + breach post-mortems | Architect + Head of CS |
| All hands | Quarterly | Roadmap update + demo | CTO |

---

## Functional requirements

### FR-1: Multi-modal unified ingestion

**Description**: The platform must ingest three telemetry streams into a single Bronze layer:
- GPU job start + completion events (Kafka + boto3 batch)
- Inference API request logs (Kafka streaming, also batch fallback)
- Per-GPU hourly node metrics (CSV batch from the fleet's existing exporter)

**Acceptance criteria**:
- [x] Each source writes to `s3://bronze/<source>/` with consistent key conventions
- [x] Kafka topics are pre-provisioned with `replication.factor=3`, `min.insync.replicas=2` (no auto-create)
- [x] Producer schemas are documented and version-controlled
- [x] A daily 50K-job batch + a continuous 100 req/s inference stream + a 6.6M-row node-metric batch can all land in Bronze without manual intervention

**Priority**: Must Have

**Dependencies**: Kafka cluster, MinIO/S3 buckets, network access from producers

**Implementation**: [ingestion/](ingestion/), [spark/jobs/streaming_consumer.py](spark/jobs/streaming_consumer.py), [data/generator/](data/generator/)

**User stories**:
1. As a **platform engineer**, I want every source going through one ingestion contract so I have one set of monitoring + one runbook.
2. As a **DPO**, I want a single Bronze layer so I have a single place to apply retention policies (Art. 5 storage limitation).

---

### FR-2: Idempotent handling of late-arriving completions

**Description**: ~15% of GPU job-completion events arrive hours (up to 24 h) after the corresponding `started_at` event. The Silver pipeline must MERGE these into the existing record without producing duplicates or overwriting completed jobs.

**Acceptance criteria**:
- [x] Re-running the daily DAG produces no duplicate `job_id` rows in `silver.jobs`
- [x] A completion event for a job that already has `ended_at` set does **not** overwrite the existing values (MERGE condition `AND t.ended_at IS NULL`)
- [x] In-flight jobs (no completion yet) show accrued cost based on `started_at` - `CURRENT_TIMESTAMP`

**Priority**: Must Have

**Implementation**: [spark/jobs/bronze_to_silver.py](spark/jobs/bronze_to_silver.py) - `merge_completions()`; [dbt/models/intermediate/int_job_costs.sql](dbt/models/intermediate/int_job_costs.sql) - Rule 4.

**User stories**:
1. As a **finance analyst**, I want late-arriving completions to update the correct day's cost, so monthly reconciliation matches the customer's actual usage.

---

### FR-3: Quality-gated promotion (Silver - Gold)

**Description**: A failed Great Expectations checkpoint on Silver must block the dbt Gold build for that day. The previous day's Gold remains queryable.

**Acceptance criteria**:
- [x] GX checkpoint runs as a `PythonOperator` between Silver write and dbt run
- [x] On failure, an Airflow `BranchPythonOperator` routes the DAG to `notify_quality_failure` and skips dbt
- [x] On success, the DAG proceeds to `dbt run` and `dbt test`
- [x] Suite asserts 9 distinct expectations covering row counts, nulls, unique keys, value ranges, drift on mean cost, accepted GPU types/frameworks
- [x] Suite version-controlled in [quality/expectations/suite_silver_jobs.py](quality/expectations/suite_silver_jobs.py)

**Priority**: Must Have

**Dependencies**: FR-2 (Silver populated), Great Expectations 0.18.19, Airflow

**Implementation**: [quality/checkpoints/silver_checkpoint.py](quality/checkpoints/silver_checkpoint.py), [orchestration/dags/batch_pipeline_dag.py](orchestration/dags/batch_pipeline_dag.py)

---

### FR-4: Cost attribution mart

**Description**: A Gold table `cost_attribution` must aggregate billable GPU spend to the `(date, org, user, model_arch, gpu_tier, gpu_type, framework)` grain, with documented billing rules for in-flight jobs, OOM kills, and platform errors.

**Acceptance criteria**:
- [x] Surrogate key `attribution_id = MD5(grain columns)` for idempotent re-runs
- [x] `total_cost_usd - 0` enforced by custom dbt test [`assert_positive_costs.sql`](dbt/tests/assert_positive_costs.sql)
- [x] OOM kills (exit_code = 137) treated as billable; platform errors (exit_code = 143) as non-billable
- [x] In-flight jobs (`ended_at IS NULL`) accrue cost from `started_at` to `CURRENT_TIMESTAMP`
- [x] Available via `GET /cost/orgs`, `GET /cost/models`

**Priority**: Must Have

**Implementation**: [dbt/models/marts/cost_attribution.sql](dbt/models/marts/cost_attribution.sql), [dbt/models/intermediate/int_job_costs.sql](dbt/models/intermediate/int_job_costs.sql), [docs/business/COST-MODEL.md](docs/business/COST-MODEL.md)

---

### FR-5: Streaming inference observability

**Description**: Per-model latency percentiles + SLO breach flag, with the streaming path monitored every 15 minutes by the streaming health DAG.

**Acceptance criteria**:
- [x] Streaming consumer writes `bronze/inference_stream/` as Delta with 30-second micro-batches
- [x] `job_performance_sla` Gold mart exposes p50 / p95 / p99 latency per `(log_date, model_id, region)`
- [x] `slo_p99_breached` flag computed from per-model SLO thresholds
- [x] `streaming_health_check` DAG runs every 15 min, alerts on Kafka lag > 10,000 or Delta staleness > 10 min

**Priority**: Must Have

**Implementation**: [spark/jobs/streaming_consumer.py](spark/jobs/streaming_consumer.py), [orchestration/dags/streaming_health_dag.py](orchestration/dags/streaming_health_dag.py), [dbt/models/marts/job_performance_sla.sql](dbt/models/marts/job_performance_sla.sql)

---

### FR-6: Self-serve REST API

**Description**: A FastAPI service exposes Gold marts as REST endpoints with OpenAPI documentation, no SQL skill required for consumers.

**Acceptance criteria**:
- [x] Endpoints: `/health`, `/metrics/summary`, `/cost/orgs`, `/cost/models`, `/utilization/hourly`, `/utilization/capacity`, `/sla/models`, `/sla/trends`
- [x] Swagger UI at `/docs`
- [x] Sub-second response on the 90-day Gold dataset
- [x] Documented schema in [docs/architecture/API.md](docs/architecture/API.md)

**Priority**: Must Have

**Implementation**: [serving/main.py](serving/main.py), [serving/routers/](serving/routers/), [docs/architecture/API.md](docs/architecture/API.md)

---

### FR-7: Horizontal scaling without downtime

**Description**: Worker pools (Celery + Spark) must scale up or down without restarting the platform.

**Acceptance criteria**:
- [x] `make scale-workers N=4` adds Celery workers; Flower shows the new workers within seconds
- [x] `make scale-spark N=4` adds Spark workers; master UI shows them
- [x] No DAG re-deploy required; `WORKER_CONCURRENCY=4` per worker

**Priority**: Should Have

**Implementation**: [Makefile](Makefile), [docker-compose.yml](docker-compose.yml), [ADR-002](docs/architecture-decisions/adr/002-airflow-vs-prefect.md)

---

### FR-8: End-to-end lineage

**Description**: Every Gold column must be traceable back to its Bronze source through documented file paths and dbt's DAG.

**Acceptance criteria**:
- [x] `dbt docs generate && dbt docs serve` renders a clickable DAG
- [x] Manual lineage table in [docs/governance/GOVERNANCE.md](docs/governance/GOVERNANCE.md) covers the cost path end-to-end
- [x] No table appears without a documented derivation

**Priority**: Should Have (GDPR Art. 30 enables this)

**Implementation**: [dbt/](dbt/), [docs/governance/GOVERNANCE.md § Lineage](docs/governance/GOVERNANCE.md)

---

### FR-9: Time-travel audit trail

**Description**: Silver and Gold Delta tables must support querying any version from the past 7 days.

**Acceptance criteria**:
- [x] `delta.logRetentionDuration` - 7 days
- [x] `VACUUM RETAIN 168 HOURS` runs nightly (not less - would break time-travel)
- [x] Example queries documented in [docs/governance/GOVERNANCE.md § Delta time travel](docs/governance/GOVERNANCE.md)

**Priority**: Should Have

**Implementation**: [spark/jobs/optimize_tables.py](spark/jobs/optimize_tables.py), [docs/governance/GOVERNANCE.md](docs/governance/GOVERNANCE.md)

---

### FR-10: Reproducible infrastructure as code

**Description**: Buckets, topics, DAGs, and dbt models must all be provisioned by code. No clicking in consoles.

**Acceptance criteria**:
- [x] `terraform apply` creates 4 buckets (bronze / silver / gold / checkpoints) with documented lifecycle rules
- [x] Same `.tf` retargets to real AWS S3 by swapping the provider
- [x] Kafka topics created explicitly by `kafka-init` (auto-create off)
- [x] DAGs version-controlled in [orchestration/dags/](orchestration/dags/)

**Priority**: Must Have

**Implementation**: [infrastructure/terraform/](infrastructure/terraform/), [ADR-004](docs/architecture-decisions/adr/004-terraform-for-iac.md)

---

## Non-functional requirements

### Performance

**NFR-P1: API latency**
- **Requirement**: P95 < 500 ms, P99 < 1 s on Gold marts at current scale ( - 90 days, - 50K jobs / day)
- **Measurement**: FastAPI middleware logs request duration; Prometheus histogram (planned)
- **Validation**: `curl -w '%{time_total}' http://localhost:8000/metrics/summary` returns < 0.5 s on a warm DuckDB

**NFR-P2: Batch throughput**
- **Requirement**: Full daily DAG (sense - Spark - GX - dbt - optimise) completes in < 20 min on a 2-worker Spark cluster
- **Measurement**: Airflow DAG runtime
- **Validation**: Observed ~12 min on local Docker Compose with 2 Spark workers × 1 GB

**NFR-P3: Streaming end-to-end lag**
- **Requirement**: Kafka enqueue - Bronze Delta visible < 60 s (P95)
- **Measurement**: `_stream_ingested_at - kafka_timestamp` distribution
- **Validation**: 30 s `processingTime` trigger + checkpoint commit - ~30 - 5 s typical

**NFR-P4: Resource utilisation**
- **Requirement**: Spark workers run at 60 - 0% memory utilisation under normal load; not OOM
- **Measurement**: Container `docker stats`; Spark UI
- **Validation**: Documented memory tuning in [README.md](README.md) and [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md)

### Scalability

**NFR-S1: Horizontal scaling**
- **Requirement**: Scale from 2 to N Spark + Celery workers without code change
- **Approach**: `make scale-workers N=…` (Docker Compose `--scale`); EKS managed node groups in production
- **Validation**: 4-worker test - 4× concurrent task throughput in Flower

**NFR-S2: Data volume**
- **Requirement**: Handle 50K jobs + 500K inference logs daily; design supports 50× growth
- **Approach**: Partitioning by date; Kafka 6 partitions per topic; Delta `OPTIMIZE` + ZORDER
- **Validation**: Synthetic generator scales to configurable `--days` and `--jobs-per-day`; documented scale-up path in [README.md § DuckDB trade-off](README.md)

### Availability

**NFR-A1: Uptime**
- **Requirement**: 99.5% (3.6 h/month) for the dev/MVP build; 99.95% (21.9 min/month) for the production target
- **Measurement**: Airflow DAG success rate + API health-check
- **Validation**: Local stack is single-host (no real HA); production HA path documented in [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md)

**NFR-A2: Disaster recovery**
- **RPO**: < 1 h (Kafka 7-day retention + Bronze 90-day retention let us replay)
- **RTO**: < 4 h (Terraform re-provisions infra; dbt rebuilds Gold from Silver in minutes)
- **Approach**: Multi-region S3 replication (production); Delta time-travel for accidental writes

**NFR-A3: Fault tolerance**
- **Requirement**: Single-broker Kafka failure causes no data loss
- **Approach**: RF=3, min.insync.replicas=2, `acks=all` on durability-critical producer
- **Validation**: Documented in [README.md § Key engineering patterns #6](README.md)

### Security

**NFR-SEC1: Authentication & authorization**
- **Requirement** (production): Enterprise SSO (SAML / OIDC) on every UI; RBAC at query layer
- **Today**: Username / password defaults; gap documented
- **Validation**: [docs/governance/SECURITY.md § Authentication](docs/governance/SECURITY.md)

**NFR-SEC2: Encryption**
- **At rest**: SSE-KMS on S3 (production); MinIO unencrypted (dev)
- **In transit**: TLS 1.2+ end-to-end (production); plaintext bridge (dev)
- **Gap**: Explicit in [docs/governance/SECURITY.md § Encryption](docs/governance/SECURITY.md)

**NFR-SEC3: Network**
- **Requirement** (production): Private subnets; VPC endpoint to S3; zero-trust proxy in front of API
- **Today**: Docker bridge; documented gap

**NFR-SEC4: Audit logging**
- **Requirement** (production): Centralised logs with 7-year retention for compliance
- **Today**: FastAPI access logs + Airflow task logs - local volumes; production wiring documented

### Compliance

**NFR-C1: GDPR**
- **Articles covered structurally**: 5 (purpose / storage limitation), 25 (privacy by design), 30 (records of processing - lineage), 32 (security of processing)
- **Articles requiring further work**: 17 (right to erasure), 20 (portability), 33 (breach notification)
- **Validation**: Per-article gap analysis in [docs/governance/GOVERNANCE.md § GDPR](docs/governance/GOVERNANCE.md)

**NFR-C2: SOC 2 Type II**
- **Trust criteria covered**: Processing Integrity (strong); Security, Availability, Confidentiality (structural)
- **Gap analysis**: [docs/governance/GOVERNANCE.md § SOC 2](docs/governance/GOVERNANCE.md)

### Cost

**NFR-COST1: Capital expenditure**
- **Budget**: $2.4M one-time
- **Allocation**: $1.6M infrastructure (EKS, MSK, S3, RDS, KMS); $0.5M migration (consultants + transition); $0.3M tooling licences (none required - fully OSS stack)
- **Today**: Local dev cost - $0 (Codespaces or laptop)

**NFR-COST2: Operating expenditure**
- **Budget**: $1.8M annually
- **Breakdown** (production projection):
 - $0.6M compute (EKS + Spark workers)
 - $0.4M storage (S3, lifecycle-tiered)
 - $0.2M streaming (MSK or Confluent Cloud)
 - $0.3M observability + secrets + IAM
 - $0.3M FTE-time for ongoing platform engineering
- **Optimisation target**: 10% YoY reduction via reserved capacity, lifecycle tiering, query optimisation

**NFR-COST3: Cost predictability**
- **Requirement**: Monthly variance < 10%
- **Approach**: Reserved instances for steady-state; cost budgets per environment; alert on > 20% MoM growth in any line item
- **Validation**: Cost model documented in [docs/business/COST-MODEL.md](docs/business/COST-MODEL.md) and [docs/business/BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md)

### Usability

**NFR-U1: Developer experience**
- **Requirement**: `make up` - all 14 services running in < 10 min on first run (image download dominated)
- **Onboarding**: Quickstart in [README.md § Quickstart](README.md) gets a new engineer to "API responds" in 15 min
- **Documentation**: 11 markdown documents totalling ~30K words

**NFR-U2: Operations**
- **Requirement**: 90% of routine operations are scripted; Airflow handles retries; Flower exposes worker state
- **Automation**: `make` targets cover up / down / scale / pipeline / test / lint / ci
- **Monitoring**: Airflow UI, Spark UI, Kafka UI, Flower, MinIO Console all exposed locally

---

## Constraints

### Technical constraints

1. **Cloud providers**: AWS is primary; abstraction layer (S3-compatible storage, K8s for compute) keeps GCP / Azure migration tractable but not free.
2. **Orchestration**: Kubernetes in production (EKS). Helm charts to be added in Phase 4.
3. **Compliance**: SOC 2 Type II, GDPR, eventually FedRAMP Moderate for the federal vertical.
4. **Integration**: Must coexist with existing Postgres scheduler DB and S3-based inference log dump during a 6-month transition.

### Organisational constraints

1. **Timeline**: 6 months MVP, 12 months full rollout, 18 months SOC 2 Type II audit.
2. **Team**: 4 FTE platform engineers + 1 architect (this document's author); dotted-line to 2 data engineers in customer organisations.
3. **Skills**: Python, Spark, dbt, Airflow strong; Kubernetes mid-level; Rust / Go: none (rules out building custom kernel-level pieces).
4. **Process**: All changes via PR + review; production deploys via GitHub Actions; ADRs for any change crossing module boundaries.

### Financial constraints

1. **Capital budget**: $2.4M maximum (board-approved)
2. **Operating budget**: $1.8M annually
3. **ROI requirement**: Breakeven by month 18; 2× by month 30 - see [docs/business/BUSINESS-CASE.md § ROI](docs/business/BUSINESS-CASE.md)

---

## Assumptions

| # | Assumption | Impact if invalid |
|---|---|---|
| A1 | GPU-pricing schedule is stable enough that quarterly review of the price map is sufficient | Mispricing window up to 90 days; mitigation: GX `mean(cost_usd) - [1, 500]` drift detector catches step-changes within 24 h |
| A2 | Customer organisations are happy with daily billing close, real-time intra-day not required at MVP | If wrong: must add incremental dbt materialisations + accrued-cost hourly job (already designed - see [COST-MODEL.md § Scaling considerations](docs/business/COST-MODEL.md)) |
| A3 | Inference log volume stays - 1B requests / month for the next 12 months | If exceeded: MSK partition count bump, Spark workers scale-out, possible Kafka tiered storage |
| A4 | DuckDB performance acceptable for serving layer at < 1B Gold rows | Already documented limit: at - 10 GB Gold tables we migrate to Spark SQL / Databricks SQL Warehouse |
| A5 | Synthetic data distribution is representative enough for design validation | Pre-prod load test with real-shape data before GA |
| A6 | All customers accept SOC 2 Type II at month 18 rather than month 6 | If wrong: prioritise SSO + KMS + TLS in Phase 2 instead of Phase 3 |

---

## Risks

| Risk | Impact | Probability | Mitigation |
|---|---|---|---|
| **R1** - DuckDB `delta` extension's non-thread-safe FFI causes prod-scale failures | High | Medium | Documented; pinned `threads: 1` for prod; migration path to Spark SQL ready ([README.md § DuckDB trade-off](README.md)) |
| **R2** - Late-arrival rate exceeds 24 h, breaking the MERGE assumption | Medium | Low | Watermark in streaming = 10 min; batch DAG re-reads Bronze for 7 days; documented |
| **R3** - Bad GPU price ships to production (cost regression) | High | Low | GX `mean(cost_usd) - [1, 500]` + dbt `assert_positive_costs.sql` |
| **R4** - Kafka cluster failure during streaming | High | Low | RF=3, min.isr=2; `acks=all` on critical topic; documented [README.md § pattern 6](README.md) |
| **R5** - SOC 2 audit fails due to missing audit logs | High | Medium | Phase 3 deliverable: ship Airflow + FastAPI logs to CloudWatch with 7-yr retention |
| **R6** - Talent gap: no Rust skills, can't fix the DuckDB FFI ourselves | Medium | High (already true) | Treat as a black-box bug; have a parallel Spark SQL path ready as fallback |
| **R7** - Cost overrun in cloud bill (MSK + S3 + EKS) | Medium | Medium | Cost-budget alerts; quarterly reserved-instance review; lifecycle tiering on Bronze |
| **R8** - Data scientists bypass the platform (back to scripts on raw S3) | Medium | Medium | Invest in API ergonomics + sample notebooks early; co-design with data-science lead |
| **R9** - GDPR right-to-erasure request received before erasure pipeline is built | Medium | Low | Phase 4 deliverable; manual SOP in interim documented in [GOVERNANCE-PROCEDURES.md](docs/runbooks/GOVERNANCE-PROCEDURES.md) |
| **R10** - Vendor lock-in via Databricks if we migrate prod Gold there | Medium | Low | Delta is open; dbt is portable; only the SQL Warehouse compute is vendor-specific |

---

## Out of scope

1. **Real customer PII** - Bronze schema design supports adding `email`, `name`, but no production loader writes them until GDPR Art. 17 erasure pipeline (Phase 4) ships.
2. **Multi-cloud at MVP** - GCP / Azure are aspirational; MVP is AWS-only.
3. **Real-time intra-day billing** - Daily close is the MVP commitment; intra-day documented as Phase 3+ work.
4. **ML model training** - This platform serves ML *infrastructure* telemetry; it is not an ML training platform.
5. **Customer-facing dashboards / UI** - REST API is the deliverable; customer-facing UI is a separate roadmap item.
6. **On-prem deployment** - Cloud-only; on-prem (e.g. for federal customers) is a Phase 5 conversation.

---

## Requirements traceability matrix

| Req ID | Business driver | Architecture component | Code | Test | NFR / ADR linked |
|---|---|---|---|---|---|
| FR-1 | Cost transparency, productivity | Kafka + boto3 + Bronze | [ingestion/](ingestion/) | manual on `make ingest` | NFR-P2 |
| FR-2 | Billing accuracy | Silver MERGE | [spark/jobs/bronze_to_silver.py](spark/jobs/bronze_to_silver.py) | [tests/unit/test_transformations.py::test_merge_late_arrivals](tests/unit/test_transformations.py) | [ADR-001](docs/architecture-decisions/adr/001-delta-lake-vs-parquet.md) |
| FR-3 | Compliance, billing accuracy | GX gate | [quality/](quality/) | suite asserts | NFR-C1, NFR-C2 |
| FR-4 | Revenue protection | dbt cost mart | [dbt/models/marts/cost_attribution.sql](dbt/models/marts/cost_attribution.sql) | [dbt/tests/assert_positive_costs.sql](dbt/tests/assert_positive_costs.sql) | NFR-COST-1 |
| FR-5 | Customer retention | Streaming + SLA mart | [spark/jobs/streaming_consumer.py](spark/jobs/streaming_consumer.py), [dbt/models/marts/job_performance_sla.sql](dbt/models/marts/job_performance_sla.sql) | streaming health DAG | NFR-P3 |
| FR-6 | Productivity | FastAPI + DuckDB | [serving/](serving/) | manual via `/docs` | NFR-P1, NFR-U1 |
| FR-7 | Reliability | Celery + Spark workers | [docker-compose.yml](docker-compose.yml), [Makefile](Makefile) | `make scale-workers N=4` | [ADR-002](docs/architecture-decisions/adr/002-airflow-vs-prefect.md), [ADR-005](docs/architecture-decisions/adr/005-docker-compose-dev-k8s-prod.md) |
| FR-8 | Compliance (Art. 30) | dbt DAG + manual table | [docs/governance/GOVERNANCE.md § Lineage](docs/governance/GOVERNANCE.md) | `dbt docs generate` | NFR-C1 |
| FR-9 | Audit, recovery | Delta time-travel | [spark/jobs/optimize_tables.py](spark/jobs/optimize_tables.py) | documented examples | NFR-A2 |
| FR-10 | Reproducibility | Terraform + IaC | [infrastructure/terraform/](infrastructure/terraform/) | `terraform apply` | [ADR-004](docs/architecture-decisions/adr/004-terraform-for-iac.md) |

---

## Acceptance criteria - overall solution

The solution is considered complete when:

- [x] All Must-Have FRs implemented and demonstrable via `make up && make pipeline`
- [x] All NFR families addressed (performance, scalability, availability, security, compliance, cost, usability)
- [x] Security gap analysis explicit (dev - prod), not glossed over
- [x] Documentation suite complete: ARCHITECTURE, API, DATA-MODEL, COST-MODEL, GOVERNANCE, GOVERNANCE-PROCEDURES, SECURITY, DEPLOYMENT, STEP_BY_STEP, BUSINESS-CASE, STAKEHOLDERS, REQUIREMENTS-ANALYSIS, PRESENTATION
- [x] 5 ADRs (target 10; remainder captured as decision tables in [docs/architecture/ARCHITECTURE.md § Key design decisions](docs/architecture/ARCHITECTURE.md))
- [x] Working end-to-end on a 16 GB laptop or GitHub Codespace
- [ ] Pre-prod load test with real-shape data (Phase 4)
- [ ] SOC 2 Type II audit (month 18)

---

## Appendices

### Appendix A - Glossary

| Term | Definition |
|---|---|
| **Bronze / Silver / Gold** | Medallion lakehouse layers - raw / cleaned-enriched / business-ready |
| **MERGE** | Delta Lake UPSERT operation; enables idempotent late-arrival handling |
| **MERGE condition** | `t.job_id = s.job_id AND t.ended_at IS NULL` - only updates open rows |
| **GX** | Great Expectations - declarative data-quality framework |
| **SLO** | Service Level Objective - internal target (e.g. p99 < 2 s) |
| **OOM** | Out-Of-Memory; exit_code = 137 in our taxonomy |
| **Platform error** | exit_code = 143 - TechCorp's fault, not billed |
| **Grain** | The unique-row identifier of a table |
| **Attribution_id** | MD5 surrogate key on the cost grain |

### Appendix B - Reference resources

- Databricks: "Medallion Lakehouse Architecture" (public blog)
- Delta Lake whitepaper: "Delta Lake: High-Performance ACID Table Storage Over Cloud Object Stores" (VLDB 2020)
- Kreps, J.: "I Heart Logs" (O'Reilly, 2014)
- Great Expectations documentation v0.18.x
- dbt-duckdb adapter documentation v1.7.4
- AWS Well-Architected Framework - Analytics Lens
- SOC 2 Trust Services Criteria (TSC) - AICPA 2017 (rev. 2022)
- GDPR - Regulation (EU) 2016/679

---

**Next step**: Review the architecture in [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md), or jump to the business framing in [docs/business/BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md).
