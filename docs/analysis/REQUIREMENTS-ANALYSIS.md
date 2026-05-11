# Requirements Analysis

> Phase 1 deliverable for project-304. Reverses the requirements out of the implementation, validates them against scenarios, traces them to architecture components, and inventories assumptions + risks. Read alongside [REQUIREMENTS.md](../../REQUIREMENTS.md) (the requirements spec) and [BUSINESS-CASE.md](../business/BUSINESS-CASE.md) (the why).

---

## Purpose

This document answers four questions a reviewer asks of any architecture submission:

1. **Where did the requirements come from?** (Driver �?requirement chain)
2. **Are they internally consistent?** (Scenario validation)
3. **What are we assuming, and what breaks if those assumptions are wrong?** (Assumption register)
4. **What can go wrong, and what's the mitigation?** (Risk register)

---

## 1. Driver �?requirement chain

Every requirement in [REQUIREMENTS.md](../../REQUIREMENTS.md) traces back to one of four business drivers. The chain below makes that mapping explicit �?useful when an executive asks "why is FR-X in scope?"

```
                       ┌────────────────────────────────────────�?
   Cost transparency   �?FR-1 (unified ingestion)               �?
   (revenue            �?FR-2 (late-arrival MERGE)              �?
   protection)         �?FR-4 (cost mart)                       �?
                       �?FR-9 (time-travel for audit)           �?
                       �?NFR-COST-1/2/3                         �?
                       └────────────────────────────────────────�?
                       ┌────────────────────────────────────────�?
   SLO credibility     �?FR-5 (streaming + SLA mart)            �?
   (customer           �?FR-6 (REST API: /sla/*)                �?
   retention)          �?NFR-P-3 (streaming lag < 60 s)         �?
                       �?NFR-A-3 (Kafka fault tolerance)        �?
                       └────────────────────────────────────────�?
                       ┌────────────────────────────────────────�?
   Self-serve          �?FR-6 (REST API)                        �?
   analytics           �?FR-7 (horizontal scaling)              �?
   (productivity)      �?NFR-U-1/2 (DX, ops)                    �?
                       �?NFR-P-1 (API latency)                  �?
                       └────────────────────────────────────────�?
                       ┌────────────────────────────────────────�?
   Compliance          �?FR-3 (quality gate)                    �?
   (regulatory)        �?FR-8 (lineage)                         �?
                       �?FR-9 (time-travel)                     �?
                       �?NFR-SEC-1/2/3/4                        �?
                       �?NFR-C-1/2 (GDPR, SOC 2)                �?
                       └────────────────────────────────────────�?
```

If a driver gets de-prioritised by leadership, the linked requirements are the candidates to defer.

---

## 2. Scenario validation

A requirement set is only useful if it survives concrete scenarios. The five below are the ones used to pressure-test the design.

### Scenario A �?"The midnight late completion"

**Setup**: A long-running training job started at 09:00 UTC on day D. The scheduler's webhook is delayed; the completion event lands at 02:30 UTC on day D+1.

**What must happen**:
1. Day D's batch DAG (01:00 UTC D+1) sees the job as still running, accrues cost from `started_at` to `CURRENT_TIMESTAMP` (FR-4 Rule 4).
2. Day D's `cost_attribution` row shows the accrued estimate.
3. Day D+1's batch DAG (01:00 UTC D+2) reads the new completion event, MERGEs it into `silver.jobs` with the condition `t.ended_at IS NULL`, updates the row's `ended_at`, `duration_hours`, `cost_usd`.
4. Day D+1's `cost_attribution` (built from current `silver.jobs`) shows the **final** cost on the original `job_date`.
5. The day-D row in `cost_attribution` is **rebuilt** from updated Silver �?finance's monthly invoice draws from the rebuilt rows.

**Requirements exercised**: FR-1, FR-2, FR-4, FR-9.

**Verdict**: �?Designed for. The dbt `cost_attribution` model is a full refresh (not incremental) �?it always rebuilds from the current state of Silver, so updates to old rows propagate automatically.

**Edge case**: If a completion arrives > 7 days late, the watermark on streaming + Bronze 90-day retention give us a 7-day �?90-day recovery window via manual replay. Documented in [README.md § Failure modes](../../README.md).

---

### Scenario B �?"The bad GPU price"

**Setup**: An H100 SXM5 price update doubles the per-hour rate from $4.25 to $8.50 due to a typo in a config push. The price reaches `int_job_costs` on day D.

**What must happen**:
1. `silver.jobs` gets the new price as `price_per_gpu_hour` for jobs starting after the change.
2. `bronze_to_silver.py` writes Silver, then the GX checkpoint runs.
3. GX expectation `expect_column_mean_to_be_between("cost_usd", 1.0, 500.0)` detects the distribution shift: mean cost jumps from ~$80 to ~$160. The expectation fails.
4. Airflow `gx_silver_quality_check` task fails �?`BranchPythonOperator` routes to `notify_quality_failure`.
5. dbt does **not** run. Gold remains at day D-1.
6. On-call investigates, sees the price change, reverts the config, re-runs DAG.

**Requirements exercised**: FR-3, FR-4, NFR-C-1.

**Verdict**: �?Designed for. The drift expectation specifically targets pricing bugs �?the cost-attribution path's quality gate has a single-direction sensor (mean cost) that catches both a too-low and a too-high regression.

**Limit**: If only a single rare GPU type is mispriced and contributes < 1% of total cost, the mean wouldn't shift enough to fail. Mitigation: future GX suite should include per-`gpu_type` mean checks.

---

### Scenario C �?"The Kafka broker dies mid-stream"

**Setup**: At peak inference load (100 req/s), one of three Kafka brokers crashes.

**What must happen**:
1. RF=3, `min.insync.replicas=2` �?producer keeps producing (still meets ISR threshold).
2. Spark Structured Streaming consumer's Kafka client transparently fails over to surviving brokers.
3. Checkpoint state in `s3a://checkpoints/inference_stream/` is unaffected; on the next 30 s trigger, consumer resumes from last committed offsets �?no duplicate writes due to exactly-once via Delta tx log.
4. Streaming health DAG's 15-min check sees Kafka lag uptick (normal during failover), then recovery.
5. No data loss; no producer error.

**Requirements exercised**: NFR-A-3, FR-5.

**Verdict**: �?Designed for. Documented in [README.md § pattern 6](../../README.md) and [docs/SECURITY.md § Kafka durability](../governance/SECURITY.md).

---

### Scenario D �?"Data scientist asks 'what was our spend on llama-3-70b last week?'"

**Setup**: A data scientist needs cost breakdown for one model family over the last 7 days, including which orgs drove the spend.

**What must happen** (today, with platform):
1. DS opens `http://api/docs`, finds `/cost/models?query_date=YYYY-MM-DD&days=7`.
2. Calls it. Gets JSON in < 500 ms.
3. For org breakdown: calls `/cost/orgs?query_date=...&days=7`, filters client-side to `model_arch="llama-3-70b-instruct"`.
4. Time-to-answer: < 5 minutes.

**What must happen** (today, without platform):
1. File a ticket with data engineering: "I need cost for llama-3-70b last week, by org".
2. DE writes a one-off SQL query against the Postgres scheduler DB; joins to the inference S3 logs manually (CSV); reconciles by hand.
3. Delivers answer 4 days later, ±8% accuracy.

**Requirements exercised**: FR-4, FR-6, NFR-P-1, NFR-U-1.

**Verdict**: �?Designed for. Self-serve is the single biggest productivity win �?the API was deliberately scoped to cover the top-5 questions DS / Finance / CS ask most.

---

### Scenario E �?"GDPR right-to-erasure request arrives"

**Setup**: A customer org's engineer leaves and asks for their `user_id`'s data to be deleted within 30 days.

**What must happen** (today):
1. Manual procedure: identify `user_id` in IAM; document in deletion log.
2. Run an ad-hoc `DELETE FROM silver.jobs WHERE user_id = '<id>'` via Spark.
3. Re-run dbt to rebuild `cost_attribution` from new Silver state.
4. Verify in `gold/cost_attribution` that no rows remain for that user.
5. Sign-off in deletion log; respond to subject within 30 days.

**What must happen** (production target �?Phase 4):
1. Subject submits request via web form �?row added to `subject_deletion_requests` queue.
2. Daily Spark job consumes queue, executes `DELETE` against Silver, triggers Gold rebuild.
3. Compliance dashboard shows queue depth + SLA compliance.

**Requirements exercised**: FR-8 (lineage), FR-9 (time-travel), NFR-C-1 (GDPR Art. 17).

**Verdict**: ⚠️ **Partial.** The manual SOP works for today's volume but doesn't scale; the production path is on the Phase 4 roadmap.

---

## 3. Assumption register

Every requirement rests on some assumption. The ones below are the load-bearing ones �?if invalidated, the design needs to change.

| ID | Assumption | Source | Validation method | Impact if invalid | Owner |
|---|---|---|---|---|---|
| A1 | GPU pricing stable enough for quarterly review | Finance Director | GX `mean(cost_usd) �?[1, 500]` drift check | Mispricing window up to 90 days; recovery via batch replay | Finance Director |
| A2 | Daily billing close sufficient for MVP | Finance Director | Confirm with CFO | If intra-day required: build accrued-cost hourly job (already designed) | Architect |
| A3 | Inference volume �?1B requests / month | Customer success forecasts | Track actual rate vs. forecast monthly | MSK partition bump; possible tiered storage | Platform Eng |
| A4 | DuckDB serving acceptable at < 1B Gold rows | Internal benchmark | Quarterly perf review | Migrate to Spark SQL / Databricks SQL Warehouse | Architect |
| A5 | Synthetic data representative of production | Generator design | Pre-prod load test with shadowed real data | Re-tune Spark configs; revisit partition counts | Data Eng lead |
| A6 | SOC 2 Type II audit at month 18 (not month 6) | Compliance roadmap | Confirm with CISO + CFO | Reshuffle Phase 2 to front-load SSO + KMS + TLS | CISO |
| A7 | OOM (137) is billable, platform error (143) is not | Finance policy | Quarterly billing-rules review with Finance Director | Re-code `int_job_costs.sql`; alert customers of rules-change | Finance Director |
| A8 | 7-day Delta time-travel sufficient for audit | DPO + CISO | Quarterly compliance review | Extend `delta.logRetentionDuration`; revisit `VACUUM RETAIN` cadence | DPO |
| A9 | Kafka 7-day retention sufficient for replay | Platform Eng | Monthly incident review | Extend to 14d; size disk accordingly | Platform Eng |
| A10 | AWS-only acceptable for MVP | CTO | Renewals + new-business review | If multi-cloud forced: abstraction is in place but Terraform modules need GCP/Azure variants | Architect |

Re-validate every quarter. Add a single-sentence note next to the assumption when the validation runs (date + outcome).

---

## 4. Risk register

10 risks, scored Impact × Probability on a 1�? scale. The full table is in [REQUIREMENTS.md § Risks](../../REQUIREMENTS.md). The summary below highlights what's worth executive attention.

### Heat map

```
              Impact �?
              1     2     3     4     5
              ──────────────────────────
Probability 5 �?             R6
�?          4 �?
            3 �?             R8    R5
            2 �?       R7    R3    R1
            1 �? R2    R4    R10   R9
```

| ID | Risk | Score (I × P) | Status |
|---|---|---|---|
| R1 | DuckDB non-thread-safe FFI causes prod failures | 4 × 2 = 8 | Mitigated (pinned `threads: 1` + fallback path) |
| R2 | Late arrivals > 24 h breaks MERGE | 2 × 1 = 2 | Mitigated (watermark + retention) |
| R3 | Bad GPU price reaches Gold | 4 × 2 = 8 | Mitigated (GX drift + dbt test) |
| R4 | Kafka cluster failure | 2 × 1 = 2 | Mitigated (RF=3, min.isr=2) |
| R5 | SOC 2 audit fails (missing audit logs) | 4 × 3 = 12 | **Open** �?Phase 3 deliverable |
| R6 | No Rust skills, can't fix DuckDB FFI | 3 × 5 = 15 | **Open** �?fallback path in place but no fix |
| R7 | Cloud-bill overrun | 3 × 2 = 6 | Open �?budget alerts in place |
| R8 | Data scientists bypass platform | 3 × 3 = 9 | Open �?sample notebooks + onboarding planned |
| R9 | GDPR erasure before automated pipeline | 4 × 1 = 4 | Mitigated (manual SOP documented) |
| R10 | Vendor lock-in via Databricks | 3 × 1 = 3 | Mitigated (Delta + dbt are open) |

### Watch list (review monthly)

- **R5** �?Until centralised audit logs ship (Phase 3), every quarter without them adds audit-fail probability. Track Phase 3 percent-complete; escalate if delayed > 1 sprint.
- **R6** �?Acknowledged tax. If the DuckDB delta-kernel-rs FFI gets a thread-safety fix upstream, immediate win. Subscribe to the GitHub issue tracker.
- **R8** �?Run a quarterly "DS satisfaction" 5-question survey: did you use the platform? Did it answer your question? If not, why?

---

## 5. Requirements-traceability �?extended

The table in [REQUIREMENTS.md § Traceability](../../REQUIREMENTS.md) maps each requirement to architecture component + code + test. The extended view below adds **the validation evidence** for every Must-Have FR �?what specifically demonstrates the requirement is satisfied.

| Req | Validation | Evidence |
|---|---|---|
| FR-1 | `make ingest` succeeds; data appears in all four Bronze prefixes | `mc ls local/bronze/` after `make ingest`, or MinIO Console at :9001 |
| FR-2 | Re-running `make pipeline` produces same row count in `silver.jobs` | `SELECT COUNT(*) FROM delta_scan('s3://silver/jobs')` before & after re-run |
| FR-3 | Inject a bad row �?DAG branches to `notify_quality_failure`; Gold unchanged | Manually corrupt a Bronze file with cost = 9999.99; run DAG; observe branch |
| FR-4 | `assert_positive_costs.sql` passes; `cost_attribution` rows match generator's expected totals | `dbt test --select assert_positive_costs` |
| FR-5 | After `make stream-live`, `/sla/models` returns updated p99 within 60 s of new requests | Time the cycle manually |
| FR-6 | `GET /docs` renders Swagger; all 8 endpoints respond < 500 ms | Manual smoke test |
| FR-7 | `make scale-workers N=4` �?Flower shows 4 workers; DAG runs use them | Flower at :5555 |
| FR-8 | `dbt docs generate` renders DAG with no orphan models | `dbt docs serve` and click the cost path |
| FR-9 | `SELECT * FROM delta_scan('s3://silver/jobs', version_as_of => 1)` returns prior state | Manual via DuckDB CLI |
| FR-10 | `terraform apply -refresh-only` shows no drift | Run after MinIO is up |

---

## 6. Open questions

Tracked here so reviewers can see what's deliberately deferred rather than overlooked.

1. **Per-GPU-type cost drift** �?Today's GX checks the global mean. Should be per-`gpu_type` mean to catch single-type regressions. Backlog.
2. **Multi-tenancy at the API layer** �?Today no auth; production must support per-org filtering. Phase 3.
3. **Backfill orchestration** �?Manual today. Phase 4: Airflow `Variable` or dynamic DAG that takes `(start_date, end_date)` and rebuilds Silver+Gold for the range.
4. **Real-time intra-day billing** �?Not committed; assumption A2 is that daily is sufficient. If invalidated, the accrued-cost hourly model is the path forward.
5. **Catalog (DataHub / Amundsen / Unity)** �?Lineage is end-to-end traceable today via dbt + paths, but there's no central searchable catalog. Likely Phase 4.
6. **Privacy-preserving inference logging** �?If prompts / completions ever get logged, need a redaction pre-stage. Not in scope today.

---

## 7. Sign-off checklist

Phase 1 is complete when:

- [x] Stakeholders identified (see [STAKEHOLDERS.md](../stakeholders/STAKEHOLDERS.md))
- [x] Drivers mapped to requirements
- [x] All Must-Have FRs validated against �?1 scenario
- [x] Assumptions documented with owner + validation cadence
- [x] Risk register with score + status
- [x] Traceability matrix complete with validation evidence
- [x] Out-of-scope explicitly listed in [REQUIREMENTS.md § Out of scope](../../REQUIREMENTS.md)
- [ ] Reviewed with CTO + VP Eng (pending)
- [ ] Sign-off from CISO on security scope (pending)
- [ ] Sign-off from CFO on cost envelope (pending)

---

**Next**: Move to architecture design �?start with [ARCHITECTURE.md](../architecture/ARCHITECTURE.md), then 5 ADRs in [adr/](../architecture-decisions/adr/).
