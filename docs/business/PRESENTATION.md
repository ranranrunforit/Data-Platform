# Presentation Material

> Executive deck outline, speaker notes, demo script, and anticipated Q&A for Project 304. This is written to match the project as it exists today: a working local lakehouse platform with strong documentation and governance foundations, plus explicit production-hardening gaps.

---

## Presentation goal

This presentation should do four things in 8–12 minutes:

1. Explain the business problem in concrete terms.
2. Show that the architecture is appropriate and already implemented.
3. Prove the project is governed, not just engineered.
4. End with a credible investment and rollout story.

The audience can be mixed: course assessor, technical reviewer, executive-style stakeholder, or teammate reading the material after the fact.

---

## Recommended deck structure

### Slide 1 — Title and one-line pitch

**Title**
`TechCorp AI Infrastructure Data Platform`

**Subtitle**
`A governed lakehouse for cost transparency, SLO visibility, and self-serve analytics`

**What appears on the slide**
- One architecture thumbnail
- One-line value statement
- Your name / project identifier / duration

**Speaker notes**
“TechCorp currently has AI infrastructure telemetry split across separate systems for jobs, inference, and node metrics. This project unifies those sources into one governed data platform so finance, customer success, and engineering can work from the same trusted data.”

### Slide 2 — The business problem

**Slide content**

| Pain | Today | Business impact |
|---|---|---|
| Billing close is manual | 5-day close, ±8% reconciliation error | Revenue leakage and low trust |
| SLO breaches are detected too late | Customers often notice first | Renewal risk |
| Analytics is not self-serve | Data scientists wait days | Productivity drag |
| Governance is weak | No strong lineage or quality gate | Audit and compliance gap |

**Speaker notes**
“The key here is not that the company lacks data. It has data everywhere. The problem is fragmentation, weak controls, and slow access.”

### Slide 3 — What the platform does

**Slide content**
- Batch ingestion of job events and node metrics
- Streaming ingestion of inference telemetry
- Bronze → Silver → Gold medallion flow
- Great Expectations gate before Gold
- FastAPI endpoints for cost, utilization, and SLA

**Speaker notes**
“This is not just a design exercise. The repository contains a working implementation using Spark, Kafka, Delta Lake, dbt, Great Expectations, Airflow, DuckDB, FastAPI, Terraform, and Docker Compose.”

### Slide 4 — Architecture overview

**Slide content**
- Kafka + raw file ingestion
- Bronze object storage
- Silver Delta tables with schema enforcement and MERGE
- Gold marts built by dbt
- Airflow orchestration
- DuckDB + FastAPI serving layer

**Speaker notes**
“The architecture uses a standard enterprise lakehouse pattern because it fits the problem well. Delta Lake gives ACID guarantees and MERGE for late-arriving job completions, while dbt provides transparent business logic and testing.”

### Slide 5 — Governance and control points

**Slide content**
- Bronze: permissive landing zone
- Silver: schema enforcement + GX quality gate
- Gold: dbt schema tests + custom singular tests
- Lineage via dbt DAG + manual lineage trace
- Retention by layer

**Speaker notes**
“The strongest differentiator of this project is that governance is operationalised, not appended at the end. A failed Silver checkpoint blocks Gold promotion, which means billing and executive dashboards fail closed instead of silently drifting.”

### Slide 6 — What is implemented versus what is planned

**Slide content**

| Already implemented | Planned hardening |
|---|---|
| Local end-to-end platform | SSO / OIDC |
| Quality-gated batch pipeline | TLS everywhere |
| Streaming freshness monitoring | KMS-backed encryption |
| Cost, SLA, and utilization APIs | Centralised audit logging |
| Governance runbooks and SOPs | Automated subject-erasure workflow |

**Speaker notes**
“This slide matters because it keeps the narrative honest. The current platform is development-grade but architecturally sound. The production path is explicit and documented.”

### Slide 7 — Business case

**Slide content**
- Capex: $2.4M
- Annual opex: $1.8M
- Payback: month 18
- 2-year ROI: 220%
- Conservative 3-year NPV: ~$2.7M

**Speaker notes**
“The ROI is built from three hard-dollar sources: reduced billing leakage, faster self-serve analytics, and lower customer-impact from SLO blind spots. I kept the compliance upside separate so the numbers remain defensible.”

### Slide 8 — Stakeholders and outcomes

**Slide content**

| Stakeholder | Outcome |
|---|---|
| CTO | Standardised platform with clear roadmap |
| CFO | Trusted cost attribution and faster close |
| CISO / DPO | Better lineage, retention, and evidence |
| VP Engineering | Repeatable pipelines and recovery path |
| Data Science | Faster access to trusted analytics |

**Speaker notes**
“A useful architecture submission should show not only what the platform is, but who it serves and how communication changes by audience.”

### Slide 9 — Delivery roadmap

**Slide content**
1. Phase 1: requirements, stakeholders, risks
2. Phase 2: architecture and implementation baseline
3. Phase 3: governance, security, deployment hardening
4. Phase 4: production hardening and operationalisation

**Speaker notes**
“The roadmap is intentionally phased so the MVP creates value early, while compliance and operational maturity continue in parallel.”

### Slide 10 — Closing

**Slide content**
- One trusted platform
- One governed path from telemetry to decision
- One realistic migration path from dev to production

**Speaker notes**
“The strongest final takeaway is that the project solves a real enterprise problem with a concrete implementation, not just aspirational architecture.”

---

## Short version: 5-minute presentation

If you only have 5 minutes, use this sequence:

1. Slide 1 — title and pitch
2. Slide 2 — the four pains
3. Slide 4 — architecture overview
4. Slide 5 — governance and controls
5. Slide 7 — business case
6. Slide 10 — close

That version prioritises business framing, architecture credibility, and governance maturity.

---

## Demo script

This is the safest live-demo path because it shows value without requiring too many moving parts.

### Demo flow

1. Show the Airflow DAGs in the Airflow UI.
2. Show the MinIO Bronze / Silver / Gold structure.
3. Open FastAPI Swagger at `/docs`.
4. Call `/metrics/summary`.
5. Call `/cost/orgs?days=7`.
6. Call `/sla/models?breaches_only=true`.

### What to say

“Here is the batch pipeline with the quality gate in the middle. Here is the medallion storage layout. And here is the self-serve surface that stakeholders actually consume. The point is that the platform is end-to-end: ingestion, transformation, governance, serving.”

### Backup if the live stack is not running

Use static screenshots or talk from:
- [3042README.md](../3042README.md)
- [docs/ARCHITECTURE.md](ARCHITECTURE.md)
- [docs/API.md](API.md)
- [docs/GOVERNANCE.md](GOVERNANCE.md)

---

## Speaker guidance by audience

### If the audience is technical

Emphasise:
- MERGE for late arrivals
- Delta Lake and exactly-once semantics
- dbt model layering
- failure modes and recovery

De-emphasise:
- headline ROI numbers

### If the audience is executive

Emphasise:
- the four pains
- governance and trust
- payback timeline
- roadmap and production gap honesty

De-emphasise:
- implementation details like partition counts or Spark settings

### If the audience is compliance/security-heavy

Emphasise:
- lineage
- retention controls
- subject-erasure SOP
- explicit gap analysis in SECURITY and GOVERNANCE docs

De-emphasise:
- performance benchmarking unless asked

---

## Likely questions and strong answers

### “Why not just buy Databricks end-to-end?”

Answer:
“Because the goal here is to prove the architecture and its controls, not to outsource thinking. The chosen stack keeps storage, transformations, and governance portable while still leaving room to adopt managed compute later if scale demands it.”

### “Is this production-ready?”

Answer:
“Not fully. It is development-grade by design. What is production-ready is the architectural direction and the governance model. Security hardening items such as SSO, TLS, KMS, and central audit logging are explicitly documented as next-phase work.”

### “What makes this more than a data pipeline?”

Answer:
“The governance layer. This project treats quality gates, lineage, ownership, retention, runbooks, and stakeholder communication as first-class deliverables rather than side notes.”

### “What is the biggest current limitation?”

Answer:
“The biggest gap is production hardening, not functional coverage. The platform works end-to-end locally, but operational controls such as authentication, encryption, and audit centralisation still need to be added for a real enterprise deployment.”

---

## Material checklist

- [x] Executive summary: [3042README.md](../3042README.md)
- [x] Full business case: [BUSINESS-CASE.md](BUSINESS-CASE.md)
- [x] Stakeholder framing: [STAKEHOLDERS.md](STAKEHOLDERS.md)
- [x] Governance framework: [GOVERNANCE.md](GOVERNANCE.md)
- [x] Governance procedures: [GOVERNANCE-PROCEDURES.md](GOVERNANCE-PROCEDURES.md)
- [x] Requirements analysis: [REQUIREMENTS-ANALYSIS.md](REQUIREMENTS-ANALYSIS.md)
- [x] Architecture narrative: [ARCHITECTURE.md](ARCHITECTURE.md)

---

## Recommended close

Use one of these final lines:

- “This project turns fragmented AI telemetry into a governed decision platform.”
- “The architecture is valuable because it improves trust, not just throughput.”
- “The submission demonstrates both system design and the operational discipline needed to run it.”
