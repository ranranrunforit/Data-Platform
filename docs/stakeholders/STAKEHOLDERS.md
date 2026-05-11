# Stakeholders

> Who cares about this platform, what they care about, how to talk to each of them, and what artefact serves each audience. Read this before any review meeting.

---

## Quick reference

| Role | Top concern | What they want from you | Artefact |
|---|---|---|---|
| **CTO** | Strategic alignment, ROI, build-vs-buy | "Why this, why now, what does it cost, when does it pay back?" | [BUSINESS-CASE.md](BUSINESS-CASE.md), [PRESENTATION.md § Exec deck](PRESENTATION.md) |
| **CFO / Finance Director** | TCO, billing accuracy, audit | "Where is the money going, and can I trust the numbers?" | [COST-MODEL.md](COST-MODEL.md), [BUSINESS-CASE.md § ROI](BUSINESS-CASE.md) |
| **VP Engineering** | Reliability, scalability, on-call load | "Will my team be paged at 3 a.m.? Can it scale?" | [ARCHITECTURE.md](ARCHITECTURE.md), [adr/](adr/), [DEPLOYMENT.md](DEPLOYMENT.md) |
| **CISO** | SOC 2, GDPR, audit trail | "What's encrypted, who has access, what's the gap?" | [SECURITY.md](SECURITY.md), [GOVERNANCE.md](GOVERNANCE.md) |
| **DPO** | GDPR Arts. 5, 17, 30, 32 | "Lineage, retention, erasure — show me." | [GOVERNANCE.md § GDPR](GOVERNANCE.md), [GOVERNANCE-PROCEDURES.md](GOVERNANCE-PROCEDURES.md) |
| **Head of Customer Success** | Renewal risk, breach response | "When do I learn about SLO breaches?" | [API.md § /sla](API.md), live dashboard demo |
| **Data Science lead** | Latency to insight, self-serve | "Can I answer 'what was our spend last week' in 5 minutes?" | [README.md](../README.md), [API.md](API.md), [STEP_BY_STEP.md](STEP_BY_STEP.md) |
| **Capacity Planning** | Fleet utilisation, hardware mix | "Which GPUs are saturated, which are idle?" | [API.md § /utilization](API.md) |
| **Platform Engineering (operator)** | Deploy / scale / debug | "What do I run when it breaks?" | [DEPLOYMENT.md](DEPLOYMENT.md), [GOVERNANCE-PROCEDURES.md § Runbooks](GOVERNANCE-PROCEDURES.md) |

---

## Detailed stakeholder profiles

### CTO — Executive Sponsor

- **Primary KPIs**: platform ROI, time-to-market for downstream products, technical debt reduction
- **Decision rights**: cap budget, kill / continue
- **Concerns**:
  - Are we building or buying? (Why not Databricks SQL Warehouse end-to-end?)
  - Multi-cloud insurance — what's lock-in?
  - Headcount efficiency — does this absorb or release engineers?
- **What you say**:
  - "Build for the lakehouse layer (Delta Lake + dbt + DuckDB + Spark) — all open, all portable. Buy managed compute (EKS, MSK) — operational toil isn't differentiating."
  - "Vendor lock-in is on managed compute only. Storage format (Delta), transformation (dbt), orchestration (Airflow) are all OSS — exit cost is migration of a few hundred lines of YAML."
  - "MVP releases 1 DE from the ticket queue and absorbs 1 platform engineer. Net: zero headcount change, 12 data scientists unblocked."
- **What you avoid saying**:
  - "Cutting-edge" — they want boring + working, not novel
  - Implementation details (Spark partition counts) — they trust you on those
- **Cadence**: Monthly 30-min review with dashboard, ad-hoc on major risk surfaces

---

### CFO / Finance Director — Budget Owner & Customer

- **Primary KPIs**: billing close cycle, reconciliation accuracy, cost-per-customer
- **Decision rights**: opex allocation, billing-rules approval
- **Concerns**:
  - Can I trust the numbers in `cost_attribution`? Where do they come from?
  - What happens if a GPU price changes mid-month?
  - How do I audit a single customer's invoice?
- **What you say**:
  - "Every `cost_attribution` row traces to source events in `silver.jobs` — `attribution_id = MD5(grain)`. Drill-down is a single dbt model lookup."
  - "Price changes are caught two ways: (a) GX checks `mean(cost_usd)` is in [$1, $500] — a 2× price typo fails the gate before Gold updates. (b) `assert_positive_costs.sql` blocks negative billing."
  - "OOM kills (137) are billable per current policy; platform errors (143) are not. Both are documented in [COST-MODEL.md § Billing rules](COST-MODEL.md). If you want to change either rule, it's a 30-minute change + 1-day pipeline rebuild."
- **Cadence**: Monthly billing-mart walkthrough during MVP, monthly variance review after GA
- **Artefacts they care about**: Cost-attribution drill-down (org → user → date → job_id); monthly reconciliation report; audit log of every billing-rule change

---

### CISO — Security Lead

- **Primary KPIs**: incidents, audit findings, vulnerability response time
- **Decision rights**: security gate on production go-live, audit-finding remediation timelines
- **Concerns**:
  - What's encrypted today vs. plan?
  - Who can read what?
  - When something goes wrong, can we tell who did it?
- **What you say** (the honest version):
  - "Today is dev-grade. [SECURITY.md](SECURITY.md) lists every gap. Production checklist is one file you can review, not a folder of half-promises."
  - "Production path: SSO on all UIs, KMS on every bucket, TLS end-to-end, audit logs to CloudWatch with 7-year retention. ETA: Phase 2 = SSO + TLS; Phase 3 = KMS + audit pipeline."
  - "Processing Integrity is the SOC 2 criterion we're strongest on out of the box — 3-checkpoint quality gate, MERGE idempotency, Delta ACID, pinned versions."
- **What you avoid saying**:
  - "We'll get to it" — be specific about Phase 2 / 3 / 4
  - Hand-waving the gap — the gap is the document, not the absence of one
- **Cadence**: Bi-weekly pair review of new endpoints / DAGs; security gate before production push
- **Artefacts they care about**: [SECURITY.md](SECURITY.md) gap analysis, threat model, dev-only-conveniences list

---

### DPO — Data Protection Officer

- **Primary KPIs**: data-subject request SLA (30 days), records-of-processing completeness
- **Decision rights**: GDPR readiness sign-off, data-classification policy
- **Concerns**:
  - Can we delete a `user_id`'s data within 30 days?
  - Where is each personal-data column, and what's its retention?
  - Is lineage demonstrable to an auditor?
- **What you say**:
  - "Today's synthetic data has no real PII. Production schema supports `org_id` + `user_id` as the only identifiers — both pseudonyms mapped from IAM."
  - "Erasure today is a manual SOP (see [GOVERNANCE-PROCEDURES.md § Subject erasure](GOVERNANCE-PROCEDURES.md)) — 30-day SLA achievable at current volume. Automated pipeline is Phase 4."
  - "Lineage is end-to-end traceable: dbt DAG renders the path from `silver/jobs` → `int_job_costs` → `cost_attribution`. Plus a manual table in [GOVERNANCE.md § Lineage](GOVERNANCE.md) for an auditor who can't run dbt."
  - "Retention is per-layer: Bronze 90 d (MinIO ILM), Silver Delta time-travel 7 d, Kafka 7 d. All explicit, no implicit infinite-retention."
- **Cadence**: Quarterly compliance review; ad-hoc on subject requests
- **Artefacts they care about**: [GOVERNANCE.md § GDPR](GOVERNANCE.md) table (per-article coverage + gap), records of processing, erasure SOP

---

### VP Engineering — Technical Owner

- **Primary KPIs**: uptime, MTTR, sprint velocity
- **Decision rights**: deploy approval, on-call rotation, architectural escalations
- **Concerns**:
  - What happens at 3 a.m. when a DAG fails?
  - Can my team understand this codebase in two weeks?
  - What's the rollback story?
- **What you say**:
  - "Every failure mode has an entry in [ARCHITECTURE.md § Failure modes](ARCHITECTURE.md). Runbooks for the top-5 are in [GOVERNANCE-PROCEDURES.md § Runbooks](GOVERNANCE-PROCEDURES.md)."
  - "Rollback: Gold tables are Delta with 7-day time-travel — `delta_scan('s3://gold/cost_attribution', version_as_of => N)` for N from history. dbt re-runs are idempotent."
  - "Onboarding target: `make up && make pipeline` runs in 15 min on a clean laptop; new engineer is committing in week 1."
- **Cadence**: Weekly tech sync (Thursday)
- **Artefacts**: [ARCHITECTURE.md](ARCHITECTURE.md), [ADRs](adr/), [DEPLOYMENT.md](DEPLOYMENT.md)

---

### Head of Customer Success — Renewals & Health

- **Primary KPIs**: NRR, customer-reported incidents, breach response time
- **Concerns**:
  - When do we learn an SLO is breached?
  - Can I answer "which customer is at risk" in a 1:1 in 5 minutes?
- **What you say**:
  - "Streaming health DAG runs every 15 min; SLO breaches surface in `gold.job_performance_sla.slo_p99_breached` and via `/sla/models`."
  - "P99 latency trend per customer-facing model: `GET /sla/trends?model_id=...&days=14`. Use it in your weekly customer reviews."
  - "Future Phase 3 work: webhook from streaming health DAG to PagerDuty / Slack for breach-detected events."
- **Cadence**: Weekly during MVP, monthly after GA; ad-hoc on customer escalations
- **Artefacts**: [API.md § /sla](API.md), Grafana dashboard (planned), Phase 3 alert integration

---

### Data Science Lead — Primary User

- **Primary KPIs**: experiment-velocity (ideas per week), time-to-answer for ad-hoc questions
- **Concerns**:
  - Can I get answers without filing a ticket?
  - Is the data trustworthy?
- **What you say**:
  - "Eight REST endpoints cover the top-5 questions your team asks weekly. Live at `/docs`."
  - "Data quality: every Gold row passed a Silver GX gate + dbt schema + custom tests. If a Gold table is queryable, it's trustworthy."
  - "If you need a custom cut, dbt is open to PRs — co-design a new mart in a half-day workshop."
- **Cadence**: Weekly office hours during MVP; monthly after GA
- **Artefacts**: [README.md](../README.md), [API.md](API.md), [STEP_BY_STEP.md](STEP_BY_STEP.md), sample notebooks (planned)

---

### Capacity Planning — Fleet Utilisation Customer

- **Primary KPIs**: cluster utilisation %, time-to-procurement-decision
- **Concerns**:
  - Which GPU types are saturated? Idle?
  - Where should the next $1M of capex go?
- **What you say**:
  - "`/utilization/hourly` and `/utilization/capacity` show per-`gpu_type` saturation in real-time (well, 15-min refresh)."
  - "Cost-attribution mart also exposes `gpu_tier` mix — finance and capacity planning can align on hardware-purchase priorities from the same data."
- **Cadence**: Monthly capacity review
- **Artefacts**: [API.md § /utilization](API.md)

---

### Platform Engineering — Operator

- **Primary KPIs**: deploy frequency, change failure rate, MTTR
- **Concerns**:
  - How do I scale workers?
  - What do I run when a pipeline fails at 3 a.m.?
- **What you say**:
  - "Scale: `make scale-workers N=4` or `make scale-spark N=4` — no DAG redeploy."
  - "Runbooks: [GOVERNANCE-PROCEDURES.md § Runbooks](GOVERNANCE-PROCEDURES.md) covers the top failures. Airflow alerts (planned) wire to PagerDuty."
- **Cadence**: Embedded in eng team; weekly sync
- **Artefacts**: [DEPLOYMENT.md](DEPLOYMENT.md), [GOVERNANCE-PROCEDURES.md](GOVERNANCE-PROCEDURES.md), [Makefile](../Makefile)

---

## Audience-specific messaging — narrative templates

When presenting to each audience, follow this 3-part structure. The differences are in the framing, not the facts.

### To executives (CTO, CFO, CISO)

```
1. THE PAIN (30 s) — concrete dollar / risk number
   "Last quarter we missed $1.2M in invoicing because the manual
    billing close is 5 days and we can't catch pricing bugs."

2. THE SOLUTION (60 s) — one diagram + one outcome metric
   "Lakehouse pattern, batch + streaming, quality-gated.
    Outcome: 5-day → 1-day close, ±8% → ±0.5% accuracy, $0
    customer-reported SLO breaches."

3. THE ASK (30 s) — what, when, how much
   "$2.4M capital, $1.8M/yr opex, breakeven month 18,
    220% 2-year ROI. Phase 1 starts week 1."
```

### To architects (VP Eng + team)

```
1. THE PROBLEM SHAPE — what's hard about this
   "Late-arriving events, real-time SLO surfacing, and lineage
    that survives an audit — at the same time."

2. THE PATTERN — what we copy from where
   "Medallion lakehouse (Databricks pattern). MERGE for late
    arrivals. Exactly-once via Delta + Kafka offsets. Cross-check
    via Great Expectations gate. dbt for SQL transforms. Same
    Spark engine for batch and streaming."

3. THE TRADE-OFFS — what we accept
   "DuckDB serving is single-thread in prod (delta-kernel-rs
    FFI not thread-safe). MinIO single-instance dev only.
    `.env` secrets dev only. Each is a documented, time-bound
    trade-off."
```

### To data scientists / engineers (primary users)

```
1. WHAT YOU CAN DO TODAY — the immediate win
   "Three REST endpoints answer 'cost by org / model / SLA'
    in under 500 ms. Hit /docs for the schema."

2. WHAT YOU CAN'T (YET) — be honest
   "No prompt / completion text in inference logs yet (PII
    review pending). No streaming intra-second freshness —
    30 s end-to-end is the SLA."

3. HOW TO ASK FOR MORE — close the loop
   "Need a new cut? dbt models are PR-able. Office hours
    every Tuesday."
```

### To finance

```
1. THE COST PATH — show the math
   "GPU $/hour × hours × billing rules → silver.jobs.cost_usd
    → int_job_costs → cost_attribution → /cost/orgs"

2. THE CONTROLS — show the gates
   "GX drift detector. dbt schema + custom tests.
    Audit trail via Delta time-travel for 7 days."

3. WHAT YOU CAN ASK FOR — the contract
   "Daily refresh, ±0.5% accuracy at GA. Monthly variance < 10%."
```

### To CISO / DPO

```
1. WHAT'S IN PLACE TODAY — the structural foundation
   "Quality-gated promotion, lineage, retention policies,
    Delta time-travel for audit."

2. WHAT'S A KNOWN GAP — the explicit list
   "[SECURITY.md] enumerates: no SSO, no TLS, no KMS, no
    centralised audit logs. Phase 2/3 deliverables."

3. WHAT'S OUT OF SCOPE TODAY — be clear
   "PII redaction pre-Bronze: not in scope. Multi-region
    replication: Phase 4."
```

---

## Communication plan

| Audience | Cadence | Channel | Owner | Artefact |
|---|---|---|---|---|
| Executive committee (CTO + CFO + CISO) | Monthly | 30-min in-person or Zoom | Architect | Dashboard + 3-slide update |
| Board update | Quarterly | Written 1-pager | CTO via architect | [3042README.md](../3042README.md) as template |
| VP Engineering | Weekly | Tech sync | Architect | Sprint review notes |
| Engineering team | Daily standup + weekly retro | Slack + meeting | Eng manager | None — verbal |
| Security review | Bi-weekly | Pair review | Architect + CISO designate | PR diffs + threat-model deltas |
| Finance | Monthly | Variance review | Architect + Finance Director | Billing-mart walkthrough |
| Customer Success | Weekly during MVP, monthly after | Demo | Architect | Live SLO dashboard |
| DPO / Compliance | Quarterly | Compliance review | Architect + DPO | [GOVERNANCE.md](GOVERNANCE.md) + records of processing |
| All-hands | Quarterly | Company-wide | CTO | Roadmap update + demo |

---

## RACI for ongoing platform decisions

| Decision | CTO | CFO | CISO | DPO | VP Eng | Architect | Platform Eng |
|---|---|---|---|---|---|---|---|
| Budget overrun > 10% | A | R | I | I | C | C | I |
| New billing rule | I | A | I | I | I | R | C |
| New data class (PII / financial / public) | I | I | A | A | C | R | C |
| New endpoint / API change | I | I | C | I | A | R | C |
| New ADR (cross-module decision) | I | I | C | I | A | R | C |
| Production deploy approval | I | I | A | I | A | R | R |
| Subject-erasure request | I | I | C | A | I | R | R |
| Worker scale-up (capacity) | I | I | I | I | A | C | R |
| Incident post-mortem | I | C | C | I | A | C | R |

R = Responsible · A = Accountable · C = Consulted · I = Informed

---

## Anti-patterns to avoid

These come up in every cross-functional review. Don't be that person.

1. **"It's complicated"** — If you can't explain a design choice to a CFO in 30 seconds, the design is wrong or the framing is. Re-frame, don't retreat.
2. **"The dashboard will fix it"** — A dashboard surfaces a problem; it doesn't fix anything. Always pair a metric with an action (alert + runbook).
3. **"We'll harden it before prod"** — Production is a continuum, not a flip. List specific gates (SSO done by date X, KMS by date Y) instead.
4. **"We chose X because it's industry-standard"** — Industry-standard for whom? Tie every choice to an ADR with context + alternatives + consequences.
5. **"Trust me on the numbers"** — Always show the source. A `cost_attribution` row that can't be drilled to source events is a row finance won't trust.

---

**See also**: [BUSINESS-CASE.md](BUSINESS-CASE.md) for the dollar-and-cents framing, [PRESENTATION.md](PRESENTATION.md) for the deck outline that uses these stakeholder frames.
