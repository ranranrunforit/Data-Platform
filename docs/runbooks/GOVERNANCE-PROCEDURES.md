# Governance Procedures

> Companion to [GOVERNANCE.md](../governance/GOVERNANCE.md). That document describes **what** the governance framework is (quality framework, lineage, ownership, retention, compliance posture). This one describes **how to operate it** â€?the concrete procedures, RACIs, runbooks, and SLAs that translate the framework into day-to-day work.

If [GOVERNANCE.md](../governance/GOVERNANCE.md) answers "is data governed?", this document answers "what do I do on Tuesday morning when X happens?"

---

## Contents

1. [Governance bodies & cadence](#1-governance-bodies--cadence)
2. [RACI for governance decisions](#2-raci-for-governance-decisions)
3. [Quality SLAs](#3-quality-slas)
4. [Runbooks (top failure modes)](#4-runbooks)
5. [Standard operating procedures (SOPs)](#5-standard-operating-procedures)
6. [Change management for data assets](#6-change-management-for-data-assets)
7. [Data classification policy](#7-data-classification-policy)
8. [Audit & evidence collection](#8-audit--evidence-collection)
9. [Onboarding checklist](#9-onboarding-checklist)

---

## 1. Governance bodies & cadence

| Body | Members | Cadence | Mandate |
|---|---|---|---|
| **Data Governance Council** | Architect (chair), DPO, CISO, Finance Director, Head of CS, VP Eng | Monthly, 60 min | Policy approval, exception requests, escalations from working group |
| **Data Quality Working Group** | Architect, Data Eng lead, Platform Eng lead, 1 DS rep | Bi-weekly, 30 min | Triage GX/dbt-test failures; tune expectations; propose new tests |
| **Compliance Review** | DPO, CISO, Architect | Quarterly, 90 min | GDPR / SOC 2 gap review; audit-evidence walkthrough |
| **Incident Review** | Whoever responded + Architect + relevant domain owner | Within 5 working days of any P1/P2 incident | Blameless post-mortem; action items; runbook updates |
| **All-hands data update** | Whole org | Quarterly, 30 min | Roadmap, wins, asks |

The Council has formal decision rights. The Working Group makes operational decisions and escalates only when they hit a policy boundary.

---

## 2. RACI for governance decisions

| Decision | Architect | DPO | CISO | Finance Director | VP Eng | Data Eng lead | Platform Eng | Data Owner |
|---|---|---|---|---|---|---|---|---|
| Approve new GX expectation | C | I | I | I | I | A/R | C | C |
| Change billing rule (cost mart) | C | I | I | A | I | R | I | I |
| Promote a new column to Gold | R | C | I | I | C | A | I | C |
| Add new data class (PII / financial / public) | R | A | C | C | I | I | I | C |
| Approve schema-breaking change | C | I | I | I | A | R | I | C |
| Approve subject erasure | I | A | I | I | I | R | I | I |
| Decide retention policy change | R | A | C | C | I | I | C | C |
| Sign-off on a new ADR | A | C | C | C | C | R | C | I |
| Approve production deploy | C | I | A (for security-touching) | I | A | R | R | I |
| Acknowledge a P1 quality incident | A | I (if data subject impact) | I (if security) | C | C | R | C | I |

R = Responsible Â· A = Accountable Â· C = Consulted Â· I = Informed

"Data Owner" varies by domain â€?see ownership table in [GOVERNANCE.md Â§ Ownership](../governance/GOVERNANCE.md).

---

## 3. Quality SLAs

| Metric | SLA | Measurement | Owner | Where |
|---|---|---|---|---|
| Gold mart freshness (cost, utilisation, SLA) | < 24 h since last successful write | Airflow DAG success timestamp; queried by `streaming_health_check` | Data Eng lead | [orchestration/dags/](../../orchestration/dags/) |
| Streaming Bronze freshness (`bronze/inference_stream`) | < 10 min stale | DuckDB query `SELECT MAX(_stream_ingested_at)` | Data Eng lead | [orchestration/dags/streaming_health_dag.py](../../orchestration/dags/streaming_health_dag.py) |
| Kafka consumer lag | < 10,000 msgs total | `KafkaAdminClient.list_consumer_group_offsets()` | Platform Eng | streaming health DAG |
| Quality-gate failure resolution | Acknowledged < 2 h, root-caused < 8 h | Airflow task failure + Jira ticket; Slack/PagerDuty integration is planned | Data Eng lead | runbook Â§4.1 |
| Subject-erasure request | Completed within 30 calendar days | Deletion log + signoff | DPO + Data Eng lead | SOP Â§5.1 |
| Schema-breaking-change review | Reviewed within 2 working days of PR | GitHub PR review | Architect | Â§6 |
| Per-DAG runtime regression | Alert if > 1.5Ã— rolling-30-day median | Airflow metric (planned: prometheus exporter) | Platform Eng | dashboard |
| dbt test pass rate (weekly average) | â‰?99% | dbt artifacts log | Data Eng lead | weekly working group |

A breach of any SLA above auto-escalates to the next governance body (e.g. quality-gate SLA breach â†?Working Group; subject-erasure SLA breach â†?Council).

---

## 4. Runbooks

These cover the failure modes called out in [ARCHITECTURE.md Â§ Failure modes](../architecture/ARCHITECTURE.md). One canonical procedure per failure type. Updated after every incident review.

### 4.1 â€?GX quality gate failure

**Symptom**: Airflow DAG `batch_pipeline_daily` task `gx_silver_quality_check` is red; downstream tasks marked `skipped`; operator sees the failure in Airflow logs/UI. Slack alerting is a production hardening step, not a current implementation.

**Severity**: P2 by default; P1 if accompanied by customer-facing cost or SLA mart staleness > 24 h.

**Response**:

1. **Acknowledge** (< 2 h): on-call engineer claims the incident in the team channel or ticket.
2. **Read the GX result**: Airflow â†?DAG â†?`gx_silver_quality_check` task â†?XCom â†?`result_json`. Identify which expectation failed.
3. **Triage by expectation**:
   - `row_count_to_be_between` failed low â‡?ingestion stalled â€?check Kafka consumer lag and Bronze object count.
   - `column_mean_to_be_between(cost_usd)` failed high â‡?pricing bug â€?check most recent change to `data/generator/job_events.py` or production price map.
   - `column_values_to_be_in_set(gpu_type)` failed â‡?new hardware not yet in price map â€?add the GPU type and re-run.
   - `column_values_to_be_unique(job_id)` failed â‡?dedup regression â€?check `bronze_to_silver.py` for recent changes.
4. **Decide**:
   - If a real data issue: open Jira ticket, do **not** force the gate, fix upstream, re-run DAG.
   - If a tuning issue (expectation is wrong, e.g. legitimately new GPU type): create PR adjusting the expectation, route through Working Group review.
5. **Communicate**: post resolution + ETA in the incident ticket or team channel.
6. **Post-mortem**: if P1 or if it's the third occurrence this quarter, schedule incident review within 5 working days.

**Bypass procedure** (rare): If on-call must promote bad data to Gold (e.g. for emergency customer billing), the architect approves explicitly in the incident record. Bypass is logged and reviewed in the next Council meeting.

---

### 4.2 â€?Streaming consumer crashed / stalled

**Symptom**: `streaming_health_check` DAG reports `delta_freshness > 10 min` or `kafka_lag > 10000`.

**Severity**: P2 (P1 if combined with customer-reported SLO complaints).

**Response**:

1. **Check Spark UI** (Spark Master â†?applications): is the streaming app running? If not, it crashed.
2. **Read the Spark driver log**: `docker compose logs spark-master | grep -i streaming` (or in production: CloudWatch / Stackdriver).
3. **Common causes**:
   - Kafka broker failover during a write â†?consumer should recover automatically; if not, restart it (`make stream-start`).
   - OOM on the streaming driver â†?bump driver memory in [docker-compose.yml](../../docker-compose.yml) or in the K8s manifest.
   - Schema drift in incoming events â†?fail-fast schema enforcement caught it; fix the producer.
4. **Restart**: `make stream-start`. Checkpoint state in `s3a://checkpoints/inference_stream/` resumes from the last committed offset (exactly-once).
5. **Verify recovery**: 30 s after restart, `_stream_ingested_at` should advance; Kafka lag should drop.

---

### 4.3 â€?Kafka broker failure

**Symptom**: One of three Kafka brokers unhealthy in Kafka UI; ISR count drops to 2.

**Severity**: P2 (no data loss expected at min.isr=2).

**Response**:

1. **Confirm in Kafka UI** (`:8082`) that the cluster is at min.isr=2 â€?producers still writing, consumers still reading.
2. **Identify the failed broker**: `docker compose ps kafka-1 kafka-2 kafka-3` (or in production: MSK / Confluent dashboard).
3. **Restart**: `docker compose restart kafka-N` (or in MSK: failover handled by AWS).
4. **Replication catches up**: ISR returns to 3 within ~5 min.
5. **If broker won't recover**: extend partition by adding a new broker; trigger reassignment via `kafka-reassign-partitions.sh`.

**Do not**: lower `min.insync.replicas` to 1 as a "fix" â€?silent data-loss window.

---

### 4.4 â€?dbt test failure in Gold

**Symptom**: `dbt_test_gold` task in `batch_pipeline_daily` is red; Gold was written but tests failed.

**Severity**: P1 if the failing test is `assert_positive_costs.sql` (cost regression); P2 otherwise.

**Response**:

1. **Read the dbt log**: Airflow task log â†?identify failing test.
2. **Identify rows**: `dbt run-operation print_failing_rows --args '{test: assert_positive_costs}'` or query directly.
3. **Decide**:
   - If real bug: roll back via Delta time-travel â€?`INSERT OVERWRITE delta.\`s3://gold/cost_attribution\` SELECT * FROM delta.\`s3://gold/cost_attribution\` TIMESTAMP AS OF 'YYYY-MM-DD HH:MM:SS'`.
   - If test is stale: PR to adjust the test, Working Group review.
4. **Page**: P1 cost regressions wake the architect â€?pull the rip cord, don't sit on a billing bug.

---

### 4.5 â€?DuckDB delta-extension thread crash

**Symptom**: dbt run aborts with SIGABRT in `dbt-duckdb` worker; no Python stack trace.

**Severity**: P2 (build fails but data is safe).

**Response**:

1. **Confirm**: log shows `signal: SIGABRT` from a `dbt run` task using `threads: > 1`.
2. **Read [README.md Â§ DuckDB trade-off](../../README.md)** to confirm this is the known FFI issue.
3. **Fix**: ensure `dbt_project.yml` (or runtime override) uses `threads: 1` for prod target.
4. **Re-run**: `dbt run --target prod` should now succeed.
5. **Long-term**: track upstream issue in `delta-kernel-rs`. If unresolved by Q4, escalate scale-out to Spark SQL (see Phase 4 roadmap in [PRESENTATION.md](../business/PRESENTATION.md)).

---

### 4.6 â€?Bronze fills disk

**Symptom**: MinIO disk utilisation > 85%; Spark write fails with `S3Exception: InsufficientStorage`.

**Severity**: P1.

**Response**:

1. **Verify ILM rule active**: `mc ilm ls local/bronze` â€?confirms 90-day expiry.
2. **Force-expire old objects** (one-off): `mc ilm rule run local/bronze` (if MinIO supports), or run a manual sweep.
3. **Vacuum Silver** (if Silver history is over-retained too): trigger the existing `optimize_tables.py` job via Airflow or run the same Spark submit command used by the nightly optimisation task.
4. **Expand storage**: in production, EBS volume expansion or scale MinIO pool.

---

## 5. Standard operating procedures

### 5.1 â€?Subject erasure (GDPR Art. 17)

**Trigger**: Verified subject-erasure request received via legal channel.

**SLA**: Erasure complete within 30 calendar days; subject notified.

**Procedure** (manual today; automated path in Phase 4):

1. **Verify identity** of requester through Legal (DPO accountable).
2. **Identify `user_id` and `org_id`** from IAM.
3. **Log the request** in `compliance/erasure-log.md` (or production: erasure-log table): date, requester, user_id, org_id, ticket reference.
4. **Verify no legal hold** (Finance flag for ongoing dispute / audit).
5. **Execute deletion** (Data Eng lead):
   ```sql
   DELETE FROM delta.`s3://silver/jobs`        WHERE user_id = '<id>';
   DELETE FROM delta.`s3://silver/inference`   WHERE user_id = '<id>';
   ```
6. **Rebuild Gold**: trigger `dbt run --full-refresh` after the Silver delete, or re-run the batch pipeline if the date range needs to be rebuilt end-to-end.
7. **Verify**: `SELECT COUNT(*) FROM delta_scan('s3://gold/cost_attribution') WHERE user_id = '<id>'` returns 0.
8. **VACUUM** to remove physical files: `VACUUM s3://silver/jobs RETAIN 0 HOURS` (overrides default â€?log the override).
9. **Sign-off**: DPO confirms in erasure log; Legal notifies subject.
10. **Retain proof of deletion** for 7 years (compliance evidence).

**Phase 4 automation**: a `subject_deletion_requests` table receives validated requests; daily Spark job processes the queue, executes the deletion, triggers Gold rebuild, updates status.

---

### 5.2 â€?Adding a new dbt mart

**Trigger**: Stakeholder need; approved by Working Group.

**Procedure**:

1. **Spec**: open a PR with a `/docs` markdown adding a row to the table in [DATA-MODEL.md Â§ Gold](../architecture/DATA-MODEL.md): name, purpose, grain, columns, refresh cadence.
2. **Identify data class** per Â§7 (`pii`, `financial`, `internal`, `public`); add `meta.sensitivity` in dbt schema.yml.
3. **Build**:
   - Staging view if not present
   - Intermediate model if non-trivial business logic
   - Mart model
   - Schema tests in `schema.yml` (at least `unique` + `not_null` on key columns)
   - At least one custom singular test if it produces dollar / SLA numbers
4. **Document the grain** in the schema.yml model description.
5. **Add the endpoint** to [serving/routers/](../../serving/routers/) if user-facing.
6. **Update lineage table** in [GOVERNANCE.md Â§ Lineage](../governance/GOVERNANCE.md).
7. **Working Group review**: 1 working day SLA.
8. **Deploy**: standard PR merge â†?main â†?CI runs `dbt build` â†?green â†?production DAG picks up next run.

---

### 5.3 â€?Adding a new Great Expectations expectation

**Trigger**: Bug discovered that should have been caught by quality gate; or proactive hardening from Working Group.

**Procedure**:

1. **Identify the column / metric** that should be gated.
2. **PR** to [quality/expectations/suite_silver_jobs.py](../../quality/expectations/suite_silver_jobs.py):
   - Add the expectation
   - Choose `mostly` thresholding (rare to require 100%; usually 0.99â€?.99)
   - Reasoning comment: "added after incident YYYY-MM-DD because ..."
3. **Test against historical data**: run `make gx-validate` against a known-good Silver snapshot; ensure the new expectation passes.
4. **Test against a known-bad case** if possible: synthesise a corrupt row, confirm expectation fires.
5. **Working Group review**: 1 working day SLA.
6. **Deploy**: PR merge â†?next DAG run uses the new suite.

**Anti-pattern**: do NOT add an expectation that's so loose it never fails. The point is to catch real bugs; loose expectations create false confidence.

---

### 5.4 â€?Backfilling a date range

**Trigger**: Bug fix or new mart needs historical data.

**Procedure**:

1. **Identify the date range** (`start_date`, `end_date`) and the impacted tables.
2. **Pre-flight check**: confirm Bronze data exists for the range (Bronze 90-day retention).
3. **Backfill Silver**: run the existing Bronzeâ†’Silver job after restoring the needed Bronze data for the date range. In the current implementation this is an all-range rebuild rather than a parameterised date-window backfill.
4. **Re-run GX**: `make gx-validate`.
5. **Backfill Gold**: `dbt run --full-refresh` (full-refresh keeps the rebuilt Gold layer consistent with the rebuilt Silver snapshot).
6. **Verify**: Spot-check `cost_attribution` for the backfilled range.
7. **Document**: append to `compliance/backfill-log.md` with date, reason, executor.

---

## 6. Change management for data assets

### 6.1 â€?Schema-breaking changes

A change is **breaking** if:
- It renames or removes a Silver/Gold column
- It changes the type of an existing column in a non-widening way (e.g. INT â†?STRING)
- It changes the grain of a Gold mart
- It changes the meaning of an existing column (e.g. `cost_usd` previously included tax, now excludes)

**Procedure**:

1. **PR** with the schema diff + a 1-page Markdown describing the change, impacted consumers, and migration plan.
2. **Identify consumers** by grepping [serving/routers/](../../serving/routers/) and any known downstream dashboards.
3. **Notify consumers** at least 2 sprints before merge: post in `#data-platform`, message each owner.
4. **Plan dual-write** if necessary: keep both old and new columns until consumers cut over.
5. **Architect approval** required.
6. **Deploy**: feature-flag if possible; cut over consumers; remove old column in a subsequent PR.

### 6.2 â€?Non-breaking changes

- Adding a column (always non-breaking)
- Adding a row to a denormalised lookup
- Tightening (not loosening) a GX expectation
- Adding a test

Standard PR review by data-eng team; no architect signoff required.

### 6.3 â€?Emergency hotfix

For active production incidents:

1. Open a PR with `[HOTFIX]` prefix.
2. Single architect approval is sufficient (instead of Working Group).
3. Document the bypass: incident link, reason, follow-up tasks.
4. **Always** schedule a post-mortem within 5 working days.
5. Working Group reviews the hotfix in their next meeting â€?confirms the follow-ups are in flight.

---

## 7. Data classification policy

Every dbt model has a `meta.sensitivity` tag. Used by the future catalog and by access-control policies.

| Class | Definition | Examples (today) | Examples (future production) |
|---|---|---|---|
| `public` | Safe to share externally without redaction | Aggregates with > 5 orgs, anonymised | Marketing dashboards |
| `internal` | TechCorp employees only | `gpu_utilization_hourly` (no per-org info) | All Gold marts not below |
| `financial` | Restricted to Finance + Architect | `cost_attribution` per-org rows | Customer invoices, internal P&L mapping |
| `pii` | Personal data subject to GDPR | None today (synthetic) | `user_id` mapping to identifiable engineer, prompt/completion text |
| `secret` | Authentication / encryption keys | None in data layer | Service-account credentials |

**Policy**:
- `pii` data: encrypted at rest (KMS) + at transit (TLS 1.2+); access logged.
- `financial` data: RBAC at API layer; no broad-cast in dashboards.
- All dbt models must have `meta.sensitivity` set; CI test enforces.
- New `pii` columns require DPO approval (RACI Â§2).

**Enforcement** (Phase 3):
- dbt-checkpoint or custom dbt test fails CI if `meta.sensitivity` missing
- FastAPI middleware checks requester's role vs. endpoint's max-sensitivity tag

---

## 8. Audit & evidence collection

What an auditor would ask for, and where it lives.

| Audit question | Evidence | Where |
|---|---|---|
| "Show me the data flow for cost." | Manual lineage table | [GOVERNANCE.md Â§ Lineage trace](../governance/GOVERNANCE.md) |
| "Show me the controls preventing bad data in billing." | GX suite + dbt tests + custom singular tests | [quality/](../../quality/), [dbt/](../../dbt/) |
| "Show me retention enforcement." | MinIO ILM rule + Kafka `retention.ms` + `VACUUM RETAIN` cadence | [GOVERNANCE.md Â§ Lifecycle](../governance/GOVERNANCE.md) |
| "Prove this row went through the quality gate." | Airflow DAG run history shows `gx_silver_quality_check` task green for that date | Airflow UI |
| "Show me change history for cost rules." | git log on `int_job_costs.sql` | `git log dbt/models/intermediate/int_job_costs.sql` |
| "How would you delete a user's data?" | SOP Â§5.1 | This document |
| "How do you detect a price-update bug?" | GX `mean(cost_usd)` expectation + `assert_positive_costs.sql` | [quality/expectations/suite_silver_jobs.py](../../quality/expectations/suite_silver_jobs.py), [dbt/tests/assert_positive_costs.sql](../../dbt/tests/assert_positive_costs.sql) |
| "Who has access to financial data?" | RBAC config (today: defaults; production: SSO group â†?API role mapping) | [SECURITY.md](../governance/SECURITY.md) |
| "Show me a Delta time-travel query." | `SELECT * FROM delta_scan('s3://gold/cost_attribution', version_as_of => 1)` | demonstrable live |
| "Where is the records of processing?" | [GOVERNANCE.md Â§ GDPR](../governance/GOVERNANCE.md) + lineage trace + dbt docs | self-serve |

**Evidence collection cadence**:
- Monthly: export Airflow DAG run history; archive in `compliance/audit-evidence/<month>/`.
- Quarterly: refresh `dbt docs generate` static site; commit snapshot.
- Per-incident: post-mortem doc archived in `compliance/incidents/`.

---

## 9. Onboarding checklist

For a new engineer joining the data platform team. Each item should take < 30 min unless noted; total target: 1 working day.

### Day 1 â€?Get the platform running

- [ ] Clone repo; install Docker Desktop / Codespace; `make up` (~15 min first time)
- [ ] `make generate && make ingest && make pipeline` â€?full pipeline run (~10 min)
- [ ] Open every UI: Airflow (:8081), Spark (:8080), Kafka (:8082), Flower (:5555), MinIO (:9001), API docs (:8000/docs)
- [ ] Read [README.md](../../README.md) end-to-end
- [ ] Hit `/metrics/summary` and read the JSON

### Day 2 â€?Understand the architecture

- [ ] Read [ARCHITECTURE.md](../architecture/ARCHITECTURE.md)
- [ ] Read 5 ADRs in [adr/](../architecture-decisions/adr/)
- [ ] Read [DATA-MODEL.md](../architecture/DATA-MODEL.md)
- [ ] Walk the cost-attribution code path: producer â†?Bronze â†?Silver MERGE â†?GX â†?dbt mart â†?API endpoint
- [ ] Run `dbt docs generate && dbt docs serve`; click through the DAG

### Day 3 â€?Understand governance

- [ ] Read [GOVERNANCE.md](../governance/GOVERNANCE.md)
- [ ] Read this document end-to-end
- [ ] Read [SECURITY.md](../governance/SECURITY.md)
- [ ] Identify which Runbook (Â§4) you'd run if `batch_pipeline_daily` failed tomorrow

### Day 4 â€?Make a change

- [ ] Open a small PR: add a docstring to one function, or add a test
- [ ] Get a review
- [ ] Merge; observe CI

### Day 5 â€?Sit with the Working Group

- [ ] Attend the bi-weekly Working Group; understand the current backlog
- [ ] Pick a starter issue from the backlog (good first issue tag in the repo)

---

**See also**: [GOVERNANCE.md](../governance/GOVERNANCE.md) for the framework, [SECURITY.md](../governance/SECURITY.md) for the security-specific gaps, [STAKEHOLDERS.md Â§ RACI](../stakeholders/STAKEHOLDERS.md) for the broader project RACI.
