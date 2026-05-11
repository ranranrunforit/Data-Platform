# Data Model

Schema reference for every table in the platform — Bronze, Silver, and Gold. Read top-to-bottom to follow how raw events become billing analytics.

---

## Source datasets (synthetic generators)

All three generators live in [data/generator/](../data/generator/) with deterministic seeds (so re-runs produce identical data).

### `gpu_job_events` — 50 000 records

One record per GPU training job. Distributions are based on MLCommons benchmarks and public GPU pricing.

| Field | Type | Notes |
|---|---|---|
| `job_id` | UUID | Primary key |
| `org_id` | string | One of `org-001` … `org-020` |
| `user_id` | string | One of `user-0001` … `user-0200` |
| `gpu_type` | string | `H100-SXM5-80GB`, `A100-SXM4-80GB`, `A100-PCIe-40GB`, `RTX-4090`, `A10G` |
| `gpu_count` | int | Weighted: 1 (30%), 2, 4, 8, 16, 32, 64 — tail of multi-node jobs |
| `framework` | string | `pytorch`, `jax`, `tensorflow`, `deepspeed` |
| `model_arch` | string | `llm-7b`, `llm-13b`, `llm-70b`, `diffusion-xl`, `diffusion-base`, `vit-large`, `bert-large`, `custom` |
| `dataset_size_gb` | double | Log-normal around `model_arch` baseline |
| `started_at` | timestamp | Random within the last 90 days |
| `ended_at` | timestamp \| null | NULL for ~15% of jobs (late-arrival simulation) |
| `duration_hours` | double \| null | Log-normal; NULL on late arrivals |
| `exit_code` | int \| null | Mostly 0; occasional 1, 137 (OOM), 143 (platform error) |
| `cost_usd` | double \| null | `duration_hours × gpu_count × price_per_hour`; NULL on late arrivals |
| `is_late_arrival` | bool | Flag for the ~15% of jobs whose completion event is sent separately |

GPU pricing (USD per hour, per GPU):

| GPU | $/hr |
|---|---:|
| H100-SXM5-80GB | 4.25 |
| A100-SXM4-80GB | 3.00 |
| A100-PCIe-40GB | 2.21 |
| A10G | 0.90 |
| RTX-4090 | 0.74 |

### `inference_api_logs` — 500 000 records

One record per inference request. Latency is bimodal — cache hits (~33 ms) vs full generation (hundreds to thousands of ms).

| Field | Type | Notes |
|---|---|---|
| `request_id` | UUID | Primary key |
| `org_id` | string | `org-001` … `org-020` |
| `user_id` | string | `user-0001` … `user-0200` |
| `model_id` | string | `llama-3-70b-instruct`, `llama-3-8b-instruct`, `mixtral-8x7b-instruct`, `stable-diffusion-xl`, `whisper-large-v3`, `llama-3-405b-instruct`, `codellama-34b-instruct` |
| `region` | string | `us-east-1` (40%), `us-west-2`, `eu-west-1`, `ap-southeast-1` |
| `status_code` | int | Weighted: 200 (70%), occasional 429, 500, 503 |
| `cache_hit` | bool | 22% true |
| `input_tokens` / `output_tokens` / `total_tokens` | int | Log-normal; zero on non-200 |
| `latency_ms` | double | Bimodal; errors are fast (10–200 ms) |
| `cost_usd` | double | `total_tokens / 1000 × cost_per_1k_tokens` |
| `timestamp` | timestamp | Random within the last 90 days |

### `node_metrics` — ~6.6M records

96 nodes × 8 GPUs × 24 h × 90 d. One record per (gpu_id, hour).

| Field | Type | Notes |
|---|---|---|
| `timestamp` | timestamp | Hour bucket |
| `node_id` | string | `node-001` … `node-096` |
| `gpu_id` | string | `node-XXX-gpu-N` |
| `gpu_index` | int | 0–7 |
| `gpu_type` | string | 16 H100 nodes, 48 A100 nodes, 32 RTX-4090 nodes |
| `rack_id` | string | 8 GPUs share a rack (12 racks total) |
| `gpu_util_pct` | double | 0–100; circadian pattern (peak 14:00–22:00 UTC) |
| `memory_util_pct` | double | Correlated with `gpu_util_pct` |
| `temp_celsius` | double | 35°C idle, scales with utilisation |
| `power_watts` | double | TDP-scaled: H100=700 W, A100=400 W, RTX-4090=450 W |

---

## Bronze layer (`s3a://bronze/`)

Raw, append-only. No transformations except writing the file.

| Prefix | Format | Source | Notes |
|---|---|---|---|
| `job_events/` | JSONL | upload_to_bronze.py | One file per generator run |
| `job_completions/` | JSONL | upload_to_bronze.py | Late-arriving completion events |
| `inference_logs/` | JSONL | upload_to_bronze.py | Bulk historical seed |
| `inference_stream/` | Delta | streaming_consumer.py | Append-only, partitioned by `log_date` |
| `node_metrics/` | CSV | upload_to_bronze.py | One file per generator run |

Lifecycle: 90-day expiry on the entire `bronze/` bucket via MinIO ILM. Re-deriveable at any time from the generators.

---

## Silver layer (`s3a://silver/`)

Cleaned, deduped, enriched, partitioned. Written by `bronze_to_silver.py` ([spark/jobs/](../spark/jobs/bronze_to_silver.py)).

### `silver/jobs`

Built from `bronze/job_events/` + `bronze/job_completions/`. Partitioned by `job_date`.

Transformations applied:
- Schema enforcement (pyspark `StructType` cast)
- `dropDuplicates(["job_id"])`
- `price_per_gpu_hour` enriched from a constant map
- `is_success` derived from `exit_code` (0 = true; null = null; else false)
- `gpu_hours = duration_hours × gpu_count`
- `job_date = to_date(started_at)`
- `_ingested_at = current_timestamp()`
- `MERGE` of `job_completions` to fill in late-arriving `ended_at`, `duration_hours`, `exit_code`, `cost_usd`, recompute `gpu_hours` and `is_success`

### `silver/inference`

Built from `bronze/inference_logs/` in the current implementation. Partitioned by `log_date`.

Transformations:
- Schema cast
- `dropDuplicates(["request_id"])`
- `cache_hit` → boolean
- `is_success = (status_code = 200)`
- `log_date`, `log_hour` derived from `timestamp`

`bronze/inference_stream/` is written continuously by Structured Streaming and is monitored directly for freshness. Folding that stream into `silver/inference` incrementally is a natural next step, but it is not part of the current batch build.

### `silver/node_metrics`

Built from `bronze/node_metrics/` CSV. Partitioned by `metric_date`.

Transformations:
- Schema cast
- `dropDuplicates(["gpu_id", "timestamp"])`
- `metric_date = to_date(timestamp)`

---

## Gold layer (`s3://gold/`)

Business-ready marts built by dbt + DuckDB. Materialised as Delta tables.

### `gold/cost_attribution` — billing mart

**Grain:** one row per `(job_date, org_id, user_id, model_arch, gpu_tier, gpu_type, framework)`.

**Source path:** `silver/jobs` → `stg_jobs` (view) → `int_job_costs` (table) → `cost_attribution` (table).

The intermediate model `int_job_costs` encodes the billing rules — see [COST-MODEL.md](COST-MODEL.md) for the full logic.

| Column | Description |
|---|---|
| `attribution_id` | MD5 surrogate key over the grain columns |
| `total_jobs`, `succeeded_jobs`, `oom_jobs`, `failed_jobs`, `running_jobs` | Volume by status |
| `total_cost_usd` | Sum of `cost_usd` |
| `total_gpu_hours` | Sum of `gpu_hours` |
| `avg_cost_per_success_usd` | Mean cost over successful jobs only |
| `avg_duration_hours`, `max_duration_hours` | Successful jobs only |
| `oom_rate` | `oom_jobs / total_jobs` — signals customers needing memory-optimised instances |
| `success_rate` | `succeeded_jobs / (total_jobs - running_jobs)` |

### `gold/gpu_utilization_hourly` — capacity planning mart

**Grain:** one row per `(hour_bucket, gpu_type, gpu_tier, rack_id)`.

**Source path:** `silver/node_metrics` → `gpu_utilization_hourly`.

| Column | Description |
|---|---|
| `avg_gpu_util_pct`, `p50_gpu_util_pct`, `p95_gpu_util_pct`, `max_gpu_util_pct` | Utilisation distribution |
| `avg_memory_util_pct`, `max_memory_util_pct` | Memory pressure |
| `avg_temp_celsius`, `max_temp_celsius` | Thermal |
| `total_power_kw` | Power draw per hour |
| `active_nodes`, `active_gpus` | Reporting GPUs |
| `high_util_gpu_count` | GPUs at > 90% — capacity pressure indicator |
| `idle_gpu_count` | GPUs at < 10% — wasted capacity |
| `thermal_warning_count` | GPUs at > 85 °C |
| `high_util_rate`, `idle_rate` | Rates over `active_gpus` |

### `gold/job_performance_sla` — SLO monitoring mart

**Grain:** one row per `(log_date, model_id, region)`.

**Source path:** `silver/inference` (success only) → `stg_inference` → `job_performance_sla`.

| Column | Description |
|---|---|
| `total_requests`, `cache_hit_requests`, `cache_hit_rate` | Volume + cache effectiveness |
| `avg_latency_ms`, `p50_latency_ms`, `p95_latency_ms`, `p99_latency_ms`, `max_latency_ms` | Latency distribution |
| `p50_latency_ms_non_cached`, `p99_latency_ms_non_cached` | Latency excluding cache hits — fairer measure of generation speed |
| `avg_tokens_per_request`, `total_tokens` | Throughput |
| `total_cost_usd`, `avg_cost_per_request` | Cost |
| `model_size_tier` | `small`, `medium`, `large`, `xl` derived from `model_id` |
| `slo_p99_breached` | Pre-computed: `p99 > threshold` where threshold depends on `model_size_tier` |

SLO thresholds (must match `streaming_health_dag.py`):

| Tier | Examples | p99 threshold |
|---|---|---:|
| small | llama-3-8b, stable-diffusion-xl, whisper | 2 000 ms |
| medium | mixtral-8x7b, codellama-34b | 3 500 ms |
| large | llama-3-70b | 5 000 ms |
| xl | llama-3-405b | 8 000 ms |

---

## Tests

dbt tests (declared in [dbt/models/marts/schema.yml](../dbt/models/marts/schema.yml)):

- `attribution_id` is unique + not_null
- `total_cost_usd` is not_null and `>= 0`
- `oom_rate`, `success_rate`, `cache_hit_rate`, `high_util_rate` are between 0 and 1
- `avg_gpu_util_pct` between 0 and 100
- `p99_latency_ms > 0`
- Sources: `job_id` and `request_id` unique + not_null; `gpu_type` and `status_code` in accepted-value sets

Custom test ([dbt/tests/assert_positive_costs.sql](../dbt/tests/assert_positive_costs.sql)) — fails if any row in `cost_attribution` has `total_cost_usd < 0`.

Great Expectations suite ([quality/expectations/suite_silver_jobs.py](../quality/expectations/suite_silver_jobs.py)):

- Row count `> 500` (silent failure detector)
- Critical columns not null: `job_id`, `org_id`, `user_id`, `gpu_type`, `gpu_count`, `started_at`
- `job_id` unique
- `gpu_count ∈ [1, 512]`, `cost_usd >= 0` (mostly 0.99)
- `gpu_type ∈ KNOWN_GPU_TYPES` (mostly 0.99)
- `framework ∈ KNOWN_FRAMEWORKS` (mostly 0.98)
- `mean(cost_usd) ∈ [1.0, 500.0]` — pricing-drift detector
- `median(gpu_count) ∈ [1, 8]`
