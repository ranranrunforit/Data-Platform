# Business Case

> Why this platform exists, what pain it solves, what it costs, and when it pays back. Read alongside [STAKEHOLDERS.md](STAKEHOLDERS.md) (who) and [COST-MODEL.md](COST-MODEL.md) (the unit economics).

---

## TL;DR

| | Number |
|---|---:|
| **Capex investment** | $2.4M one-time |
| **Annual opex** | $1.8M |
| **Payback period** | 18 months |
| **2-year ROI** | 220% |
| **3-year NPV @ 10%** | ~$2.7M |
| **Annualised hard-dollar return** | ~$5.8M / yr (billing recovery + DE productivity + customer retention) |

If TechCorp does not build this, the trajectory of the four pain points below continues unchanged.

---

## 1. The pain points

Each pain is anchored to a concrete, recent, **measurable** event — not a hypothetical.

### Pain 1 — Billing reconciliation error costs real money

**Concrete instance**: Q4 last year, a manual export missed 17 large training runs because the scheduler-DB query joined on the wrong column. $1.2M of GPU spend wasn't billed. Discovered six weeks later in a CFO-mandated audit. Board lost confidence in the GPU-revenue line.

**Why it happens**:
- Cost data lives in Postgres (scheduler) but inference cost lives in S3 (logs)
- Reconciliation is a hand-written SQL + Python script run monthly by one analyst
- No automated check catches "this number looks wrong"
- No drill-down — finance can't trust a number they can't trace to source events

**Annualised cost of inaction**:
- Direct revenue leakage: $1.2M × 4 quarters ≈ **$4.8M / year** at recent miss rate
- Even if the average miss rate is half that: **~$2.4M / year**

**How this platform fixes it**:
- `cost_attribution` mart materialised daily with `attribution_id = MD5(grain)` — drill-down is one query
- GX `mean(cost_usd) ∈ [$1, $500]` detects pricing bugs within 24 h of Silver write
- `assert_positive_costs.sql` blocks negative billing
- 5-day close → 1-day close → continuous after Phase 3

---

### Pain 2 — Data scientists blocked behind a ticket queue

**Concrete instance**: 12 data scientists in the internal-ML org. 4-person data-engineering team owns all custom data pulls. Current ticket queue: 40+ open, average 4-day wait for a new cost cut.

**Why it happens**:
- No self-serve query surface — every cut requires a custom join across Postgres + S3 + InfluxDB
- No shared mart — every analyst re-derives "GPU spend by org by week" their own way
- DE team is the bottleneck, not because they're slow but because every request is a fresh problem

**Annualised cost of inaction**:
- 12 DS × 8 h / week waiting × $250 / fully-loaded hour × 50 weeks = **$1.2M / year** in idle DS time
- Plus 4 DE FTEs × 60% of time on bespoke pulls × $200K loaded cost = **$0.48M / year** in DE opportunity cost
- **Subtotal: ~$1.7M / year**

**How this platform fixes it**:
- 8 REST endpoints cover the top-5 questions DS / Finance / CS ask
- Time-to-first-insight: 4 days → 5 minutes
- DE team freed for platform work (new marts, quality rules) instead of bespoke pulls

---

### Pain 3 — SLO breaches reported by customers, not the platform

**Concrete instance**: Twice in the last six months, a customer escalated p99 latency degradation that TechCorp's own dashboards hadn't surfaced. Both customers cited it during renewal negotiations; one extracted a 12% renewal discount.

**Why it happens**:
- Inference logs land in S3 once per hour (batch upload); platform sees breaches up to 6 h late
- No streaming pipeline; no per-model p99 trend
- Customer Success has no proactive alerting — they react to escalations

**Annualised cost of inaction**:
- One renewal discount of 12% on a $4M ARR customer ≈ **$0.48M lost ARR / year**
- Customer-trust cost (qualitative but real): churn risk on the next renewal
- **Direct estimate: ~$0.5M / year**, with multiplier on renewal-cohort decisions

**How this platform fixes it**:
- Spark Structured Streaming reads `inference-api-logs` topic in 30 s micro-batches
- `job_performance_sla` mart exposes per-model p50/p95/p99 with `slo_p99_breached` flag
- Streaming health DAG runs every 15 min; alerts Customer Success via Slack (Phase 3)
- Breach detection: 6 h → < 5 min (Phase 2) → < 30 s (Phase 3)

---

### Pain 4 — Compliance posture blocks new markets

**Concrete instance**: SOC 2 Type II is the gate to TechCorp's federal-vertical strategy (2027 target market: ~$120M ARR opportunity). Today, audit-readiness work has no owner because the data plumbing isn't there.

**Why it happens**:
- No central lineage — can't answer "where does this number come from?" without a manual investigation
- No automated quality gate — auditor would have to take TechCorp's word on data integrity
- No documented retention — bronze data sits indefinitely in S3 because nobody owns the lifecycle
- No subject-erasure procedure — GDPR Art. 17 would be done by hand under deadline pressure

**Annualised cost of inaction**:
- **Foregone revenue**: $120M target federal ARR over 3 years = ~$40M / year at maturity. Even a 6-month audit delay defers ~$20M of pipeline.
- Plus residual GDPR fine risk on EU exposure: max €20M or 4% of revenue — call this $0M expected but tail-risk material

**How this platform fixes it**:
- Lineage end-to-end through dbt DAG + manual trace ([GOVERNANCE.md § Lineage](GOVERNANCE.md))
- Three-checkpoint quality framework ([GOVERNANCE.md § Quality](GOVERNANCE.md))
- Explicit retention per layer (Bronze 90 d, Kafka 7 d, Silver Delta 7 d)
- GDPR per-article gap analysis with explicit owner per gap

---

## 2. Sum of the case

| Pain | Annualised cost today | Year-1 reduction | Year-2 reduction |
|---|---:|---:|---:|
| Billing reconciliation error | $2.4M | 70% = $1.7M | 90% = $2.2M |
| DS / DE productivity drag | $1.7M | 70% = $1.2M | 85% = $1.4M |
| Customer-reported SLO breaches | $0.5M | 60% = $0.3M | 80% = $0.4M |
| Compliance (federal market gate) | $0 / yr until 2027; pipeline-defer cost ~$20M / 6-month slip | 0 at GA | $0 / yr if SOC 2 Type II ships month 18 |
| **Total hard-dollar annual return** | | **$3.2M** | **$4.0M** |
| **Plus federal-market enablement (NPV)** | | included in payback | $40M / yr at maturity (out of scope of payback math) |

Even excluding the federal-market upside (which is outside the 3-year window of this case), hard-dollar return alone breaks even on the $4.2M three-year investment by month 18.

---

## 3. The writing logic — how this case was built

This section makes the construction of the case explicit so reviewers can see the reasoning, push back on numbers, and not get sold a black-box ROI. Stakeholders are sceptical of business cases for a reason — most are reverse-engineered from a desired conclusion. This one's path is open.

### Step 1 — Anchor every pain to a concrete event

We don't say "manual billing is slow." We say "Q4 last year missed $1.2M because of a manual export bug." If a stakeholder hasn't lived that bug, they don't feel the pain.

**Test**: every pain in §1 names a specific incident, a specific dollar number, or a specific person's named time. No abstractions.

### Step 2 — Annualise the impact

Single-incident dollar numbers are easy to dismiss as one-offs. Translate to annual run-rate using the most defensible multiplier:

- For billing miss: take the worst recent quarter and assume half-rate baseline → $2.4M / yr (deliberately conservative; the actual Q4 was $4.8M annualised, but using half makes the case harder to dismiss as cherry-picking).
- For DS time: 12 DS × 8 h / wk × $250 / h × 50 wk — multiply, show the arithmetic. Sceptics can debate the 8 h, but the math is transparent.
- For SLO renewal cost: one concrete renewal-discount event with its dollar amount.

**Test**: every annualised number names its multiplier and the assumption behind it. No "we estimate" without showing the formula.

### Step 3 — Match each pain to a specific platform capability

If a pain doesn't trace to a capability in the build, **delete the pain** from the case. (Adding pains "to make the case stronger" is the classic anti-pattern.)

| Pain | Capability | File |
|---|---|---|
| Billing miss | Quality-gated cost mart | [dbt/models/marts/cost_attribution.sql](../dbt/models/marts/cost_attribution.sql) + [quality/expectations/](../quality/expectations/) |
| DS productivity | Self-serve REST API | [serving/](../serving/) |
| SLO blindness | Streaming + SLA mart | [spark/jobs/streaming_consumer.py](../spark/jobs/streaming_consumer.py), [dbt/models/marts/job_performance_sla.sql](../dbt/models/marts/job_performance_sla.sql) |
| Compliance | Lineage + retention + quality | [docs/GOVERNANCE.md](GOVERNANCE.md) |

**Test**: each pain in §1 has a § "How this platform fixes it" with code-level evidence.

### Step 4 — Be honest about Year-1 vs. Year-2 reduction

A platform doesn't go to 100% benefit on day 1. Be explicit about ramp:

- Year 1: ~70% reduction — MVP is shipped, but adoption is incomplete, automation is partial, customers still use old habits during transition.
- Year 2: ~85% reduction — adoption complete, automation production-grade.

Sceptics will probe the ramp. Sandbagging Year 1 (70%, not 100%) is the credible posture.

### Step 5 — Separate hard-dollar from strategic enablement

The federal-market enablement (compliance pain) is potentially $40M / year. It would dominate the ROI calculation if included — and that's why we don't include it in the payback math. Treat it as a **separate**, **discrete** strategic option that the platform makes available.

**Test**: the payback period (18 months) and 2-year ROI (220%) are computed **without** the federal market in the numerator. The federal opportunity is called out in §1 pain 4 but excluded from §2.

### Step 6 — Show the cost honestly

Capex $2.4M and opex $1.8M / year are not estimates — they're sized from the COST-MODEL.md cost breakdown:

| Bucket | Capex | Annual opex |
|---|---:|---:|
| EKS + Spark workers + Kafka (MSK) | $1.0M | $0.6M |
| S3 + lifecycle | $0.2M | $0.4M |
| Observability + secrets + IAM | $0.2M | $0.3M |
| Migration consultants (capex only) | $0.5M | – |
| Phase 4 hardening (capex) | $0.5M | – |
| Ongoing platform-eng FTE (3 × $200K) | – | $0.6M |
| Buffer (10%) | $0.0M | (already included) |
| **Total** | **$2.4M** | **$1.9M** rounded → $1.8M after Year-2 optimisations |

**Test**: every line traces to a specific cloud SKU or FTE rate. No "miscellaneous" line.

### Step 7 — Build a credible NPV

Year-3 NPV with the standard cost-of-capital assumption:

```
Year 0:   -$2.4M (capex)
Year 1:   -$1.8M (opex) + $3.2M (return) = +$1.4M
Year 2:   -$1.8M (opex) + $4.0M (return) = +$2.2M
Year 3:   -$1.8M (opex) + $4.5M (return) = +$2.7M

NPV @ 10% = -2.4 + 1.4/1.1 + 2.2/1.21 + 2.7/1.331
          = -2.4 + 1.27 + 1.82 + 2.03
          = $2.7M (excluding terminal value)

Plus terminal value at 5% perpetuity beyond year 3:
  = (2.7 × 1.05 / (0.10 - 0.05)) / 1.331
  = 56.7 / 1.331
  = ~$42.6M

→ Full NPV: ~$45M — but this is sensitive to terminal-value
  assumptions. The defensible headline is "3-year NPV ~$2.7M;
  >>$10M if the platform persists past year 3 (it will)".
```

We headline the conservative number ($2.7M three-year NPV) and explain the perpetuity assumption rather than the other way around.

**Test**: the headline number is the one you can defend on the worst day.

### Step 8 — Identify what would invalidate the case

Honesty about failure modes makes the case more credible, not less.

| Invalidator | Probability | If true: |
|---|---|---|
| DE productivity savings smaller (DS don't adopt the API) | Medium | Roughly halves the $1.7M; payback slips to ~24 months. Mitigation: invest in DS onboarding from week 1 (R8 in [REQUIREMENTS-ANALYSIS.md § Risks](REQUIREMENTS-ANALYSIS.md)) |
| Billing recovery smaller (audit catches more than expected) | Low | Cuts hard-dollar return by ~$1M; still pays back inside 30 months |
| Cloud bill overrun > 20% | Medium | Opex up to $2.2M; case still positive but tighter. Mitigation: reserved capacity, lifecycle tiering, quarterly review |
| SOC 2 audit slips beyond month 18 | Medium | No impact on hard-dollar return (it's excluded from payback math); affects strategic-enablement story only |

---

## 4. Success metrics (the contract)

What the platform commits to deliver. Tracked monthly; visible to all stakeholders.

| Metric | Baseline | Month 6 (MVP) | Month 12 (GA) | Month 24 |
|---|---|---|---|---|
| Time-to-first-insight (data scientist) | 4 days | 30 min | 5 min | 5 min |
| Monthly billing close | 5 days | 1 day | Continuous | Continuous |
| Reconciliation error | ±8% | ±2% | ±0.5% | ±0.5% |
| Customer-reported SLO breaches | 2 / 6 mo | 0 (platform detects first) | 0 | 0 |
| Quality incidents reaching Gold | ~2 / qtr | 0 (GX gate) | 0 | 0 |
| DS open tickets (DE queue depth) | 40+ | < 10 | < 5 | < 5 |
| Streaming end-to-end lag (P95) | n/a (no streaming today) | < 60 s | < 30 s | < 30 s |
| Platform availability | n/a | 99.5% | 99.95% | 99.95% |

Track in a single Grafana dashboard once observability lands (Phase 3). Until then, monthly slide.

---

## 5. Why now (vs. defer)

Three reasons it doesn't make sense to defer this further:

1. **The pain is compounding.** Each quarter of manual billing close burns ~$0.6M (annualised $2.4M ÷ 4). At current trajectory we cross break-even on this dimension alone in about 18 months — and we still have all the deferred federal-market upside.
2. **The federal opportunity has a 2027 audit-readiness deadline.** SOC 2 Type II takes 12 months of operating evidence. Starting the platform in 2026 means audit-ready by mid-2027 — late but viable. Starting in 2027 misses the window entirely.
3. **The technology stack is mature, not novel.** Delta Lake, Spark, Kafka, dbt, Airflow are all 5+ year-old, well-documented, well-supported tools. We are not betting on novelty; we are assembling a known pattern.

---

## 6. What we're not promising

The case excludes the following deliberately:

- **Real-time intra-day billing** — daily close is the commitment. Intra-day is a Phase 4+ conversation if customers demand it.
- **Multi-cloud at MVP** — AWS-first. GCP / Azure are aspirational; the abstraction layer keeps them tractable but not free.
- **AI-powered insights / anomaly detection** — the platform serves data, not models. Anomaly detection on top of the marts is a downstream product.
- **Customer-facing dashboards** — REST API only. Customer UI is a separate product roadmap item.
- **Inference prompt / completion text storage** — out of scope until a PII redaction pipeline is in place.

---

## 7. The ask

| | |
|---|---|
| **Approval needed** | $2.4M capex + $1.8M annual opex |
| **Headcount** | 4 platform engineers + 1 architect (this document's author) — internal reallocation, no new hires |
| **Timeline** | 6 months MVP → 12 months GA → 18 months SOC 2 Type II |
| **First gate** | End of month 3: end-to-end Bronze → Silver → Gold path in staging, quality gate active |
| **Kill criteria** | If end of month 6 (MVP gate) and < 30% of DS team has used the API in a typical week, pause and re-scope |

---

**Next**:           
[PRESENTATION.md](PRESENTATION.md) for the deck outline;           
[REQUIREMENTS-ANALYSIS.md](REQUIREMENTS-ANALYSIS.md) for the detailed risk register;           
[COST-MODEL.md](COST-MODEL.md) for the unit economics.          
