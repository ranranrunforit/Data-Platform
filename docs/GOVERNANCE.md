# Data Governance

How the platform handles data quality, lineage, ownership, and lifecycle.

---

## Quality framework

Quality is enforced at three checkpoints, with each layer making weaker guarantees acceptable than the next.

### 1. Bronze — schema-on-read

Bronze is intentionally permissive. Producers write JSONL or Delta with no schema enforcement at write time. The risk of a schema drift breaking writes is traded for the ability to keep ingesting during incidents and reprocess later.

What is enforced:
- Kafka producers serialise every value as JSON before publishing (`kafka.errors.NoBrokersAvailable` blocks startup until the broker is reachable)
- Object key conventions: `bronze/<source>/<filename>` enforced by `upload_to_bronze.py`
- Lifecycle: 90-day expiry on the `bronze/` bucket — Bronze is reproducible from generators / Kafka retention

### 2. Silver — schema-enforced + Great Expectations gate

Silver is where the platform actually fails closed. The Spark transform applies an explicit `StructType` (rejecting unparseable rows), dedupes on the primary key, and runs the [Great Expectations suite](../quality/expectations/suite_silver_jobs.py) before any Gold write is allowed:

| Expectation | Purpose |
|---|---|
| `expect_table_row_count_to_be_between(min_value=500)` | Catch silent ingestion failures |
| `expect_column_values_to_not_be_null` on `job_id`, `org_id`, `user_id`, `gpu_type`, `gpu_count`, `started_at` | Critical-field integrity |
| `expect_column_values_to_be_unique(job_id)` | Catch dedup bugs |
| `expect_column_values_to_be_between(gpu_count, 1, 512)` | Catch parser bugs |
| `expect_column_values_to_be_between(cost_usd, 0, …)` mostly 0.99 | Catch billing bugs (allow NULL on in-flight jobs) |
| `expect_column_values_to_be_in_set(gpu_type, KNOWN_GPU_TYPES)` mostly 0.99 | Catch new GPU types not yet in the price map |
| `expect_column_values_to_be_in_set(framework, KNOWN_FRAMEWORKS)` mostly 0.98 | Same for ML frameworks |
| `expect_column_mean_to_be_between(cost_usd, 1.0, 500.0)` | Distribution drift detector — catches a mispriced GPU type |
| `expect_column_median_to_be_between(gpu_count, 1, 8)` | Catches a workload mix shift |

Failure mode: the GX checkpoint exits 1, the Airflow `gx_silver_quality_check` task fails, the `BranchPythonOperator` routes to `notify_quality_failure`, and Gold is **not** updated for that day. Yesterday's Gold data remains queryable until the operator fixes the upstream issue and re-runs the DAG.

### 3. Gold — dbt schema + custom tests

dbt tests run after every Gold materialisation:

- **Schema tests** (declared in [dbt/models/marts/schema.yml](../dbt/models/marts/schema.yml)): uniqueness, not_null, accepted_values, range checks via `dbt_utils.expression_is_true`.
- **Source tests** ([dbt/models/staging/sources.yml](../dbt/models/staging/sources.yml)): same checks at the Silver source — provides a second-line defence after GX.
- **Custom singular tests** ([dbt/tests/](../dbt/tests/)): e.g. `assert_positive_costs.sql` fails if any row in `cost_attribution` has a negative `total_cost_usd`.

Failure here does **not** roll back Gold (dbt-duckdb does not support transactional rollbacks across models). It marks the DAG run as failed and pages on-call. Gold remains queryable but flagged as suspect — see "Failure modes" in [architecture.md](architecture.md).

---

## Lineage

The platform's lineage is end-to-end traceable through file paths and dbt's DAG. There is no central catalog (Datahub, Amundsen, Unity Catalog) — for a production deployment that would be the obvious next addition.

### Manual lineage trace (cost example)

| Layer | Artefact | Built by |
|---|---|---|
| Generation | `data/raw/job_events/job_events_<date>.jsonl` | `data/generator/job_events.py` |
| Bronze | `s3://bronze/job_events/job_events_<date>.jsonl` | `ingestion/upload_to_bronze.py` |
| Bronze (Kafka path) | Kafka topic `gpu-job-events` | `ingestion/kafka/producers/job_producer.py` |
| Silver | `s3://silver/jobs/` (Delta, partitioned by `job_date`) | `spark/jobs/bronze_to_silver.py::transform_jobs` |
| Silver (MERGE) | `s3://silver/jobs/` updated | `spark/jobs/bronze_to_silver.py::merge_completions` |
| Quality | exit 0 / 1 | `quality/checkpoints/silver_checkpoint.py` |
| Staging (view) | `stg_jobs` | `dbt/models/staging/stg_jobs.sql` |
| Intermediate | `int_job_costs` | `dbt/models/intermediate/int_job_costs.sql` |
| Gold | `cost_attribution` | `dbt/models/marts/cost_attribution.sql` |
| Serving | `GET /cost/orgs` | `serving/routers/cost.py` |

### dbt lineage

`dbt docs generate && dbt docs serve` renders a clickable DAG of every model, source, and test:

```
silver.jobs (source)
  └── stg_jobs (view)
       └── int_job_costs (table)
            └── cost_attribution (table)
                 └── tests: unique attribution_id, total_cost_usd >= 0,
                            assert_positive_costs

silver.inference (source)
  └── stg_inference (view)
       └── job_performance_sla (table)

silver.node_metrics (source)
  └── gpu_utilization_hourly (table)
```

### Delta time travel

Every Silver and Gold table is a Delta table with a `_delta_log/` directory. The default 7-day retention (configurable via `delta.logRetentionDuration`) lets analysts query a table as of a previous version:

```sql
-- "What did cost_attribution show before the bug was fixed?"
SELECT * FROM delta_scan('s3://gold/cost_attribution', version_as_of => 42);

-- "What did silver.jobs look like at midnight UTC?"
SELECT * FROM delta_scan('s3://silver/jobs', timestamp_as_of => '2024-01-15 00:00:00');
```

This is the platform's audit trail — every change to a Gold table is recoverable for 7 days.

---

## Ownership

| Domain | Owner | Tables / endpoints |
|---|---|---|
| Job telemetry | Data Engineering | `silver/jobs`, `gold/cost_attribution`, `/cost/*` |
| Inference telemetry | Data Engineering | `silver/inference`, `gold/job_performance_sla`, `/sla/*` |
| Node metrics | Infrastructure | `silver/node_metrics`, `gold/gpu_utilization_hourly`, `/utilization/*` |
| Pipelines | Data Engineering | All Airflow DAGs |
| Quality suites | Data Engineering | `quality/expectations/*` |

DAG `owner` is set to `data-engineering` in [batch_pipeline_dag.py](../orchestration/dags/batch_pipeline_dag.py) and [streaming_health_dag.py](../orchestration/dags/streaming_health_dag.py). In a production deployment this would be wired to PagerDuty / Opsgenie via Airflow's `email_on_failure` or a custom alert callback.

---

## Lifecycle and retention

| Layer | Retention | Mechanism |
|---|---|---|
| Bronze (raw files) | 90 days | MinIO ILM rule `mc ilm add --expiry-days 90 local/bronze` (in `minio-init`) |
| Kafka topics | 7 days | `retention.ms=604800000` set explicitly per topic by `kafka-init` |
| Silver Delta | 7-day Delta time-travel | `VACUUM RETAIN 168 HOURS` in `optimize_tables.py` |
| Gold Delta | Unlimited (for this project) | No vacuum on Gold — analysts may need historical versions |
| Spark streaming checkpoints | Indefinite | Required for exactly-once recovery; manual prune if growing |
| Airflow logs | Local volume `airflow-logs` | Compose retention; in prod: ship to S3 / CloudWatch |

---

## Personally identifiable information (PII)

The synthetic datasets contain no real PII:
- `org_id` and `user_id` are synthetic identifiers (`org-001`, `user-0042`)
- No email addresses, names, IP addresses, or payment details
- `region` is the AWS-style region code, not a customer location

In production, the same model would map to real org and user IDs from the IAM system. The cost attribution path (org → user → model) is the only place where an internal user could be re-identified — those tables would be access-controlled to finance + customer success only.

The `request_id` in inference logs is a UUID with no embedded user info. If the platform ever stored prompt or completion text, a separate redaction pass would be required before Bronze.

---

## Compliance posture

This is a development-grade platform. The sections below map each major compliance regime to the controls the platform already supports, the controls it does not, and what would be added.

### GDPR

GDPR governs personal data of EU residents. The synthetic datasets here contain no real PII, but the production version would carry org / user identifiers that may be traceable to natural persons (engineers training models inside a customer org). The relevant articles and the platform's posture:

| Article | Requirement | Today | Production gap |
|---|---|---|---|
| Art. 5 (purpose limitation) | Data collected for one purpose may not be silently re-used | Each Gold mart has a documented purpose ([DATA-MODEL.md](DATA-MODEL.md)); column-level descriptions in `dbt/models/marts/schema.yml` | Add a data-classification tag (`pii`, `internal`, `public`) to dbt model meta + enforce via `dbt-checkpoint` |
| Art. 5 (storage limitation) | Personal data must not be retained longer than necessary | Bronze 90-day MinIO ILM; Kafka 7-day retention; Silver 7-day Delta time-travel via `VACUUM RETAIN 168 HOURS` | Per-record retention based on customer contract; automated purge jobs |
| Art. 17 (right to erasure) | Subject can request deletion within 30 days | Not implemented | Add a `subject_deletion_requests` queue → daily Spark job runs `DELETE FROM silver.jobs WHERE user_id IN (...)` + cascading Gold rebuild. Delta supports `DELETE` on Silver; Gold marts must be rebuilt from new Silver snapshot |
| Art. 20 (right to portability) | Subject can request a machine-readable export | Not implemented | API endpoint `/export/user/{user_id}` joining all tables, gated by service-account auth |
| Art. 25 (privacy by design) | Minimisation by default | Inference logs do not store prompt / completion text | If text is ever stored, add a redaction pass before Bronze (e.g. Microsoft Presidio) |
| Art. 30 (records of processing) | Maintain a register of processing activities | Lineage trace section above is the start | Generate a `dbt docs` site automatically on every Gold deploy and version it |
| Art. 32 (security of processing) | Pseudonymisation, encryption, integrity, availability | Delta ACID guarantees integrity; nightly `OPTIMIZE` + `VACUUM`; quality gate prevents bad data | TLS end-to-end (S3A, Kafka, Postgres); encryption at rest; MFA on admin access — see [SECURITY.md](SECURITY.md) |
| Art. 33 (breach notification within 72 h) | Detect + notify within 72 hours | Streaming health DAG could be extended; no alert routing today | PagerDuty / Opsgenie integration on Airflow `on_failure_callback`; Falco / GuardDuty on the cluster |
| Art. 35 (DPIA) | Data protection impact assessment | n/a development | Required before processing any production user data |
| Art. 44 (cross-border transfers) | EU data may not leave EU without SCCs | `region` column tracks where data was generated; storage is currently single-region MinIO | Multi-region S3 with replication restricted by SCC-compliant routes; IAM policies pinning EU data to EU buckets |

### SOC 2 (Trust Services Criteria)

SOC 2 covers five Trust Services Criteria. The platform's coverage:

#### Security (CC1 – CC9)

| Control area | Today | Production gap |
|---|---|---|
| CC2.1 – Information & communication | This README + the `docs/` directory; ADRs document decisions | Onboarding runbooks; on-call rotation docs |
| CC5.1 – Logical access | Default `admin/admin` on Airflow, default MinIO root creds | OIDC / SAML SSO on every UI; per-org IAM roles; MFA mandatory for admin |
| CC6.1 – Logical access provisioning | None | Just-in-time access via SSO group → IAM role mapping |
| CC6.6 – Encryption | None at rest, none in transit on the bridge network | SSE-KMS on S3 buckets; TLS on every inter-service connection; managed certs via cert-manager / ACM |
| CC6.7 – Transmission of data | Plaintext within Docker | TLS 1.2+ end-to-end |
| CC6.8 – Vulnerability management | Pinned image versions in `Dockerfile.airflow`, `Dockerfile.spark`, `serving/Dockerfile`; pinned tool versions in CI | Trivy / Grype image scans in CI; Dependabot for Python deps; signed images via Sigstore |
| CC7.1 – Detection of system anomalies | `streaming_health_check` DAG monitors Kafka lag and Delta freshness | Centralised log aggregation (CloudWatch / ELK / Datadog); anomaly detection on metrics |
| CC7.2 – Incident response | Manual today | Runbook per failure mode in [architecture.md § Failure modes](architecture.md); on-call rotation |

#### Availability (A1)

| Control | Today | Production gap |
|---|---|---|
| A1.2 – System recovery | Spark task retries (2× with 5 min delay); Airflow DAG has `max_active_runs=1` to prevent overlap; Kafka RF=3, min.isr=2 — single-broker failure causes no data loss | Multi-AZ deployment; managed RDS with automated backups + PITR; cross-region S3 replication |
| A1.3 – Backup procedures | Delta time-travel (7 days); Kafka 7-day retention; Bronze 90-day retention | Off-site backups of Postgres metadata + Gold tables; quarterly recovery tests |

#### Processing Integrity (PI1)

This is the criterion the platform is strongest on out of the box.

| Control | Today |
|---|---|
| PI1.1 – Quality | Three-checkpoint quality framework (schema enforcement at Silver write, GX gate before Gold, dbt tests after Gold) |
| PI1.2 – Inputs validated | Spark `StructType` enforces input schema; Kafka producers serialise via a known schema |
| PI1.3 – Processing complete + accurate | `MERGE` for late-arriving events guarantees idempotency; Delta ACID guarantees no partial writes; pinned tool versions; deterministic seeds in tests |
| PI1.5 – Outputs reviewed | dbt schema + custom tests run after Gold; Custom test `assert_positive_costs.sql` blocks negative billing |

#### Confidentiality (C1)

| Control | Today | Production gap |
|---|---|---|
| C1.1 – Identification of confidential data | None | Tag dbt models with `meta: { sensitivity: confidential }` |
| C1.2 – Disposal | Delta `VACUUM`; MinIO ILM | Cryptographic erasure via per-tenant KMS keys |

#### Privacy (P1 – P8)

The Privacy criterion is largely a superset of GDPR for SOC 2 + Privacy. See the GDPR table above. Additional SOC 2 Privacy specifics:

| Criterion | Today | Production gap |
|---|---|---|
| P1.1 – Notice | Synthetic data; no notice required | Privacy notice published; cookie consent on any UI handling user data |
| P3.1 – Choice and consent | n/a | Consent capture for data collection beyond contractual purpose |
| P4.1 – Collection minimisation | Generators emit only fields the marts use | Annual review of every Bronze schema for fields that are no longer needed |

### What this project demonstrates today

The platform is not SOC 2 / GDPR compliant today — it is a development-grade build. What it **does** demonstrate is the structural foundation those programmes require:

- **Documented data flow** — every column has a known source ([DATA-MODEL.md](DATA-MODEL.md)) and every transformation has source code in version control.
- **Quality-gated promotion** — bad data is provably blocked from reaching analytical surfaces (GX gate + dbt tests + custom singular tests).
- **Lineage** — manual trace + dbt DAG + Delta time-travel. No table appears without a documented derivation path.
- **Idempotent re-runs** — re-running the pipeline does not produce different results given the same inputs (deterministic dedup, MERGE guarantees, surrogate keys).
- **Retention policies** — explicit at every layer (Bronze 90 d, Kafka 7 d, Silver Delta history 7 d).
- **Documented gaps** — every missing control is written down rather than implicit, so a compliance team can scope the gap exactly.

The "production gap" columns above are the explicit roadmap from development-grade to compliance-ready. Most rows resolve to existing tooling (SSO, KMS, TLS, Trivy, External Secrets Operator) wired into the K8s deployment from [ADR-005](adr/005-docker-compose-dev-k8s-prod.md), not to additional architecture decisions.
