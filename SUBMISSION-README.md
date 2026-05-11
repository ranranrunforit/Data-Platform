# Project 304 - Data Platform for AI

**Duration**: 85 hours | **Difficulty**: High | **Project ID**: project-304-data-platform

> Submission README - concrete instantiation of the course template against the actual implementation in this repository.

---

## Overview

This project designs and implements an **enterprise-grade lakehouse data platform** that ingests, governs, and serves AI infrastructure telemetry for a hypothetical Fortune-500 GPU cloud (the "TechCorp" scenario). The platform handles three concrete workloads:

- **Batch ingestion** of 50K GPU training-job events per day with ~15% late-arrival rate
- **Real-time streaming** of 500K+ inference API requests per day at ~100 req/s
- **Time-series fleet telemetry** from 96 nodes × 8 GPUs × hourly readings × 90 days (~6.6M rows)

These flow through a **medallion Bronze - Silver - Gold** lakehouse, gated by automated data-quality checks, then served via a REST API used by finance, customer success, and capacity-planning teams.

The repository contains the full working implementation (Spark, Kafka, Delta Lake, dbt, Great Expectations, Airflow, FastAPI, Terraform, Docker Compose), not just a paper design.

---

## Learning objectives - how this project satisfies them

| Objective | Where in this repo |
|---|---|
| 1. Apply enterprise architecture frameworks to a real problem | [README.md](README.md), [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md), [docs/analysis/REQUIREMENTS-ANALYSIS.md](docs/analysis/REQUIREMENTS-ANALYSIS.md) |
| 2. Create comprehensive architecture documentation and artifacts | Themed documentation in [docs/README.md](docs/README.md) + 5 ADRs in [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/) |
| 3. Design for scalability, security, and cost-effectiveness | [docs/business/COST-MODEL.md](docs/business/COST-MODEL.md), [docs/governance/SECURITY.md](docs/governance/SECURITY.md), `make scale-workers N=4`, K8s target ([ADR-005](docs/architecture-decisions/adr/005-docker-compose-dev-k8s-prod.md)) |
| 4. Communicate architecture to diverse stakeholders | [docs/stakeholders/STAKEHOLDERS.md](docs/stakeholders/STAKEHOLDERS.md), [docs/business/BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md), [docs/business/PRESENTATION.md](docs/business/PRESENTATION.md) |
| 5. Make and document architectural decisions (ADRs) | 5 ADRs covering Delta Lake, Airflow, PySpark, Terraform, Docker→K8s |

---

## Key deliverables

| Deliverable | Status | Location |
|---|---|---|
| Lakehouse architecture | - Implemented | Spark + Delta Lake + MinIO + dbt + DuckDB |
| Governance framework | - Implemented | 3-checkpoint quality gate + lineage + retention - [GOVERNANCE.md](docs/governance/GOVERNANCE.md), [GOVERNANCE-PROCEDURES.md](docs/runbooks/GOVERNANCE-PROCEDURES.md) |
| Lineage | - Implemented | dbt DAG + manual trace + Delta time-travel |
| Cost model | - Implemented | Per-GPU-hour pricing + billing rules - [COST-MODEL.md](docs/business/COST-MODEL.md) |
| Security posture | - Documented (dev-grade; prod gaps explicit) | [SECURITY.md](docs/governance/SECURITY.md) |
| Streaming pipeline | - Implemented | Spark Structured Streaming, 30 s micro-batches, exactly-once |
| REST serving | - Implemented | FastAPI + DuckDB at `:8000/docs` |
| Orchestration | - Implemented | Airflow CeleryExecutor, 2 DAGs |
| IaC | - Implemented | Terraform (MinIO - same code targets S3) |
| ADRs (10+ target) | ⚠️ 5 implemented, 5 more captured implicitly in `docs/` | [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/) |
| Implementation roadmap | - | [docs/business/PRESENTATION.md](docs/business/PRESENTATION.md) + [REQUIREMENTS-ANALYSIS.md § Roadmap](docs/REQUIREMENTS-ANALYSIS.md) |

---

## Project scenario

### Context

You are the **AI Infrastructure Architect** at **TechCorp**, a Fortune-500 GPU cloud provider competing with CoreWeave, Lambda Labs, and Together AI. The organisation has:

- **Current state**: A fleet of ~96 nodes (768 H100/A100/A10G/RTX-4090 GPUs across 4 regions). Job-scheduler logs go to Postgres; inference logs go to S3; node telemetry goes to InfluxDB. Finance bills monthly from manually-exported CSVs. Data scientists building internal models cannot self-serve - they file tickets with data engineering and wait 3 -  days.
- **Challenges**:
 - 3 disconnected telemetry stores - no single source of truth for `$ spent × utilisation × SLO`
 - Manual monthly billing - 5-day close, ±8% reconciliation error
 - No data-quality gate - a bad GPU price pushed last quarter caused a $1.2M invoicing miss
 - No real-time SLO dashboard - p99 latency breaches caught by customers, not the platform
 - Data scientists blocked behind a data-engineering ticket queue
- **Goals**:
 - Single platform unifying batch jobs, streaming inference, and node metrics
 - Self-serve analytics for data scientists, finance, customer success, capacity planning
 - Automated quality gates that block bad data from reaching billing surfaces
 - 30-second freshness on streaming SLO dashboards
 - Cost model and lineage auditable for SOC 2 / GDPR
- **Constraints**: 6-month MVP, 12-month full rollout, $2.4M capex / $1.8M opex per year, must support AWS as the eventual cloud (multi-cloud aspirational), team of 4 FTEs + 1 architect.

### Mission

Design and architect a complete solution that addresses all requirements while optimising for cost, performance, and security - and prove it works by implementing the platform end-to-end with synthetic-but-realistic data.

---

## Requirements (summary; full text in [REQUIREMENTS.md](REQUIREMENTS.md))

### Functional requirements

1. **FR-1: Multi-modal ingestion** - Ingest GPU job events (Kafka + batch), inference logs (Kafka streaming), node metrics (CSV batch) into a unified Bronze layer.
2. **FR-2: Late-arrival handling** - Job-completion events arriving up to 24 h after `started_at` must MERGE into the open record without producing duplicates.
3. **FR-3: Quality-gated promotion** - No Gold table may be updated for a day on which the Silver quality suite has failed.
4. **FR-4: Cost attribution** - Per `(date, org, user, model_arch, gpu_tier)` cost rollup with documented billing rules (including OOM and platform-error treatment).
5. **FR-5: Streaming inference observability** - Per-model p50/p95/p99 latency + SLO breach flag, refreshed every 15 minutes.
6. **FR-6: Self-serve API** - REST endpoints exposing cost, utilisation, and SLA marts.
7. **FR-7: Horizontal scalability** - Worker pool scales without downtime (`make scale-workers N=…`).
8. **FR-8: End-to-end lineage** - Every Gold column traceable to its Bronze source.
9. **FR-9: Time-travel audit** - 7-day Delta history on Silver and Gold.
10. **FR-10: Reproducible infrastructure** - Buckets / topics / DAGs provisioned by code; same Terraform targets MinIO and S3.

### Non-functional requirements (highlights)

- **Performance**: API P95 < 500 ms; streaming end-to-end lag < 60 s.
- **Scalability**: Horizontal Spark + Celery workers; Kafka 6 partitions per topic.
- **Availability**: Kafka RF=3, min.isr=2 - single-broker failure with no data loss; Spark task retries (2× / 5 min).
- **Security**: Documented gap analysis (dev - prod) for SSO, KMS, TLS, RBAC.
- **Cost**: $1.8M annual opex envelope; lakehouse projected 60% cheaper than the alternative (separate lake + warehouse) - see [BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md).
- **Compliance**: Structural readiness for SOC 2 + GDPR; explicit gap list in [GOVERNANCE.md](docs/governance/GOVERNANCE.md).

### Constraints

- **Budget**: $2.4M capital; $1.8M annual operating.
- **Timeline**: 6 months to MVP, 12 months full rollout.
- **Compliance**: SOC 2 Type II within 18 months; GDPR Art. 30 ready at GA.
- **Integration**: Must coexist with the existing Postgres scheduler DB and S3 inference logs during a 6-month transition.

---

## Project structure (this repository)

```
project-304-data-platform/
├── SUBMISSION-README.md                          # This file - course submission README
├── REQUIREMENTS.md                    # Detailed requirements
├── EXECUTIVE-SUMMARY.md                         # 1-page executive summary
├── README.md                             # Implementation README (developer-facing)
├── data/generator/                       # Synthetic data generators (3 datasets)
├── ingestion/                            # Kafka producers + boto3 - Bronze uploader
├── spark/                                # Bronze→Silver, streaming, optimise
├── dbt/                                  # Staging / intermediate / marts
├── quality/                              # Great Expectations suite + checkpoint
├── orchestration/dags/                   # 2 Airflow DAGs (batch + streaming health)
├── serving/                              # FastAPI app + routers
├── infrastructure/terraform/             # MinIO bucket provisioning (same - S3)
├── tests/unit/                           # PySpark unit tests
├── docs/                                 # Architecture, API, governance, security - -   ├── ARCHITECTURE.md
 -   ├── REQUIREMENTS-ANALYSIS.md          # Phase 1 deliverable
 -   ├── STAKEHOLDERS.md                   # Stakeholder material + comms plan
 -   ├── BUSINESS-CASE.md                  # Pain points + ROI + writing logic
 -   ├── GOVERNANCE.md                     # Quality, lineage, ownership, compliance
 -   ├── GOVERNANCE-PROCEDURES.md          # Specific procedures + runbooks + RACI
 -   ├── PRESENTATION.md                   # Executive deck outline + speaker notes
 -   ├── SECURITY.md, API.md, COST-MODEL.md, DATA-MODEL.md, DEPLOYMENT.md, STEP_BY_STEP.md
 -   └── adr/                              # 5 ADRs
├── docker-compose.yml                    # 14-service local stack
├── Dockerfile.airflow, Dockerfile.spark
├── Makefile                              # All dev/ops commands
└── .env.example
```

---

## Phase-by-phase walkthrough

### Phase 1: Requirements analysis (10 h) - **complete**

- Stakeholder analysis - [docs/stakeholders/STAKEHOLDERS.md](docs/stakeholders/STAKEHOLDERS.md)
- Requirements traceability matrix - [docs/REQUIREMENTS-ANALYSIS.md § Traceability](docs/REQUIREMENTS-ANALYSIS.md)
- Assumptions + risks register - [docs/REQUIREMENTS-ANALYSIS.md § Risks](docs/REQUIREMENTS-ANALYSIS.md)
- Business-case writing logic - [docs/BUSINESS-CASE.md § How this case was built](docs/BUSINESS-CASE.md)

### Phase 2: Architecture design (30 h) - **complete**

- High-level architecture diagram - [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md)
- Storage layout, component map, table grains - same file
- 5 ADRs - [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/)
- Data model - [docs/architecture/DATA-MODEL.md](docs/architecture/DATA-MODEL.md)
- Non-functional design (failure modes, recovery) - [docs/ARCHITECTURE.md § Failure modes](docs/ARCHITECTURE.md)

### Phase 3: Implementation planning (15 h) - **complete**

- Phased roadmap (MVP - GA - multi-cloud) - [docs/PRESENTATION.md § Roadmap](docs/PRESENTATION.md)
- Cost model + GPU pricing - [docs/COST-MODEL.md](docs/COST-MODEL.md)
- Risk register - [docs/REQUIREMENTS-ANALYSIS.md § Risks](docs/REQUIREMENTS-ANALYSIS.md)
- Success metrics + KPIs - [docs/BUSINESS-CASE.md § Success metrics](docs/BUSINESS-CASE.md)

### Phase 4: Documentation (15 h) - **complete**

- Architecture docs (8 files in [docs/](docs/))
- API specification - [docs/architecture/API.md](docs/architecture/API.md)
- Security architecture - [docs/SECURITY.md](docs/SECURITY.md)
- Governance framework - [docs/GOVERNANCE.md](docs/GOVERNANCE.md) + procedures
- Deployment guide - [docs/operations/DEPLOYMENT.md](docs/operations/DEPLOYMENT.md)

### Phase 5: Presentation (10 h) - **complete**

- Executive deck outline + speaker notes - [docs/business/PRESENTATION.md](docs/business/PRESENTATION.md)
- Audience-specific material (CTO, CFO, CISO, VP Eng, Data Science) - [docs/STAKEHOLDERS.md § Audience messaging](docs/STAKEHOLDERS.md)
- Demo script (5-min live demo) - [docs/PRESENTATION.md § Demo script](docs/PRESENTATION.md)

---

## Assessment rubric - self-assessment

### Architecture quality (40%)

| Criterion | Self-assessment | Evidence |
|---|---|---|
| Completeness | All 10 FRs and all NFR families addressed | [Requirements traceability matrix](docs/analysis/REQUIREMENTS-ANALYSIS.md) |
| Soundness | Industry-standard patterns (medallion, MERGE, exactly-once, RF=3) | [README.md § Key engineering patterns](README.md) |
| Scalability | Horizontal Celery + Spark workers; Kafka 6-partition topics; documented scale-up path | [ADR-002](docs/architecture-decisions/adr/002-airflow-vs-prefect.md), [ADR-005](docs/architecture-decisions/adr/005-docker-compose-dev-k8s-prod.md) |
| Security | Honest gap analysis dev - prod | [SECURITY.md](docs/governance/SECURITY.md) |
| Cost-effectiveness | Lakehouse 60% cheaper than lake+warehouse alternative; cost model documented | [BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md), [COST-MODEL.md](docs/business/COST-MODEL.md) |

### Documentation (30%)

| Criterion | Self-assessment | Evidence |
|---|---|---|
| Clarity | Every doc opens with "what / why / when to read this" | All [docs/](docs/) files |
| Completeness | 8 architecture docs + 5 ADRs + 3 root-level READMEs | [docs/](docs/), [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/) |
| Visual communication | Mermaid diagrams in README + ARCHITECTURE | [README.md](README.md), [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) |
| ADRs | 5 well-reasoned ADRs with context / decision / consequences | [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/) |
| Stakeholder communication | Audience-specific narratives | [docs/stakeholders/STAKEHOLDERS.md](docs/stakeholders/STAKEHOLDERS.md) |

### Strategic thinking (20%)

| Criterion | Self-assessment | Evidence |
|---|---|---|
| Business alignment | Each FR/NFR ties to a documented business driver | [BUSINESS-CASE.md § Driver-to-requirement map](docs/BUSINESS-CASE.md) |
| Long-term vision | Multi-cloud path; Kubernetes migration explicit | [ADR-005](docs/architecture-decisions/adr/005-docker-compose-dev-k8s-prod.md), [DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) |
| Risk management | Risk register with impact × probability × mitigation | [REQUIREMENTS-ANALYSIS.md § Risks](docs/REQUIREMENTS-ANALYSIS.md) |
| Innovation | DuckDB-reads-Delta zero-copy pattern; one Delta table both stream + batch consumers | [README.md § Key engineering patterns](README.md) |
| Trade-off analysis | Every major choice has an ADR; "Known trade-offs" table in ARCHITECTURE.md | [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/), [ARCHITECTURE.md § Known trade-offs](docs/ARCHITECTURE.md) |

### Implementation planning (10%)

| Criterion | Self-assessment | Evidence |
|---|---|---|
| Feasibility | Implementation runs end-to-end on a 16 GB laptop | [README.md § Quickstart](README.md) |
| Phasing | 5-phase roadmap with explicit deliverables | [PRESENTATION.md § Roadmap](docs/PRESENTATION.md) |
| Resource planning | 4 FTEs + 1 architect mapped to phases | [PRESENTATION.md § Team & timeline](docs/PRESENTATION.md) |
| Success metrics | 7 KPIs with baselines + targets | [BUSINESS-CASE.md § Success metrics](docs/BUSINESS-CASE.md) |

---

## Success criteria - confirmed

- - All functional and non-functional requirements addressed
- - Architecture aligns with enterprise medallion / lakehouse standards
- - Complete documentation suite with diagrams (Mermaid)
- - 5 ADRs documenting key decisions (target 10; remaining captured as decision tables in docs)
- - Cost model within $1.8M annual opex envelope
- - Security gap analysis explicit; structural compliance foundation in place
- - Implementation roadmap with phases
- - Executive presentation material (deck outline + speaker notes + audience-specific)
- - Peer review feedback - to incorporate after submission

---

## Tools and resources used

| Category | Tool | Why |
|---|---|---|
| Diagramming | Mermaid (in-Markdown) | Version-controlled, no external editor, renders on GitHub |
| Documentation | Markdown | Universal, diff-friendly |
| Cloud | AWS pricing data (public) for cost model | $2.21 - 4.25 / GPU-hour realism |
| Cost calculators | Manual model in [COST-MODEL.md](docs/business/COST-MODEL.md) | Cross-checked against generator's synthetic prices |
| Compute | Local Docker Compose; GitHub Codespaces; Oracle Cloud Free Tier | $0 demo-able |

### Reading drawn upon

- Databricks Medallion Architecture pattern
- Delta Lake transaction-log paper
- Kreps, "I Heart Logs" - Kafka design philosophy
- Fowler, "Patterns of Enterprise Application Architecture" - three-layer separation of concerns
- Course module content (project 304 reference materials)

---

## Timeline (actual)

| Week | Hours | Phase | Output |
|---|---|---|---|
| 1 -  | 10 | Requirements analysis | [REQUIREMENTS-ANALYSIS.md](docs/analysis/REQUIREMENTS-ANALYSIS.md), [STAKEHOLDERS.md](docs/stakeholders/STAKEHOLDERS.md) |
| 3 -  | 30 | Architecture + ADRs + implementation | Working code + [ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) + 5 ADRs |
| 7 -  | 15 | Cost modelling + risk register | [COST-MODEL.md](docs/business/COST-MODEL.md), [BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md) |
| 9 - 0 | 15 | Doc suite + governance | [GOVERNANCE.md](docs/governance/GOVERNANCE.md), [SECURITY.md](docs/governance/SECURITY.md), [API.md](docs/architecture/API.md), [DATA-MODEL.md](docs/architecture/DATA-MODEL.md), [DEPLOYMENT.md](docs/operations/DEPLOYMENT.md) |
| 11 | 10 | Presentation + polish | [PRESENTATION.md](docs/business/PRESENTATION.md), demo script, refinement |
| **Total** | **80** | | (5 h buffer used for CI fixes and ADR refinement) |

---

## Submission checklist

- - All deliverables in [docs/](docs/) - 11 documents
- - Diagrams (Mermaid) embedded in [README.md](README.md) and [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md)
- - 5 ADRs in [docs/architecture-decisions/adr/](docs/architecture-decisions/adr/)
- - Cost analysis with optimisation rationale ([COST-MODEL.md](docs/business/COST-MODEL.md), [BUSINESS-CASE.md](docs/business/BUSINESS-CASE.md))
- - Top-level [README.md](README.md) explains the implementation
- - Working code - runs via `make up` on a 16 GB laptop or Codespace

---

## Next steps

1. Read the developer-facing [README.md](README.md) for the working implementation
2. Read [REQUIREMENTS.md](REQUIREMENTS.md) for the detailed requirements
3. Read [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) for the system design
4. Read [docs/BUSINESS-CASE.md](docs/BUSINESS-CASE.md) for the why
5. Read [docs/business/PRESENTATION.md](docs/business/PRESENTATION.md) for the executive narrative

---

**Questions?** See [EXECUTIVE-SUMMARY.md](EXECUTIVE-SUMMARY.md) for the 1-page executive summary, or jump to any document linked above.

