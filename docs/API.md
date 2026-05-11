# API Reference

The serving API is a FastAPI app backed by DuckDB reading Gold Delta tables directly from MinIO. There is no caching layer between the API and storage — every request executes a fresh DuckDB query.

- **Base URL (local):** `http://localhost:8000`
- **Interactive docs:** `http://localhost:8000/docs` (Swagger UI)
- **OpenAPI schema:** `http://localhost:8000/openapi.json`
- **Source:** [serving/main.py](../serving/main.py), routers in [serving/routers/](../serving/routers/)

## How it works

On startup, the FastAPI lifespan handler builds a single DuckDB `:memory:` connection per worker and loads the `httpfs` + `delta` extensions, configured to talk to MinIO. Each handler runs a parameterised DuckDB query against `delta_scan('s3://gold/<table>')`.

There is no authentication on the local API. For production, see [SECURITY.md](SECURITY.md).

---

## Endpoints

### `GET /health`

Liveness probe.

```json
{ "status": "ok" }
```

---

### `GET /metrics/summary`

7-day dashboard summary across cost, jobs, and SLO compliance.

**Response:**
```json
{
  "period": "last_7_days",
  "cost":  { "total_usd": 184234.18, "total_gpu_hours": 47215.3 },
  "jobs":  { "total": 4123, "avg_success_rate": 0.962, "avg_oom_rate": 0.018 },
  "slo":   { "breach_count": 4, "total_model_days": 196, "compliance_rate": 0.9796 }
}
```

---

### Cost attribution

#### `GET /cost/orgs`

Cost breakdown by organisation over a date range.

| Query param | Type | Default | Description |
|---|---|---|---|
| `query_date` | date (YYYY-MM-DD) | yesterday | End date of the range |
| `days` | int (1–90) | 1 | Number of days back from `query_date` |

**Response:**
```json
{
  "start_date": "2024-01-15",
  "end_date":   "2024-01-15",
  "orgs": [
    {
      "org_id": "org-007",
      "total_cost_usd": 12834.42,
      "total_gpu_hours": 3120.5,
      "total_jobs": 287,
      "succeeded_jobs": 271,
      "oom_jobs": 4,
      "avg_oom_rate": 0.014,
      "avg_success_rate": 0.944
    }
  ]
}
```

#### `GET /cost/models`

Cost breakdown by model architecture and GPU tier.

| Query param | Type | Default |
|---|---|---|
| `query_date` | date | yesterday |
| `days` | int (1–90) | 7 |

**Response (truncated):**
```json
{
  "start_date": "2024-01-09",
  "end_date":   "2024-01-15",
  "models": [
    {
      "model_arch": "llm-70b",
      "gpu_tier":   "flagship",
      "total_cost_usd": 28471.83,
      "total_gpu_hours": 6700.4,
      "total_jobs": 142,
      "avg_cost_per_job": 200.51,
      "avg_duration_hours": 11.8
    }
  ]
}
```

---

### GPU utilization

#### `GET /utilization/hourly`

Hourly GPU utilisation for a single date.

| Query param | Type | Default | Description |
|---|---|---|---|
| `query_date` | date | yesterday | Date to inspect |
| `gpu_type` | string | (all) | Filter to one of `H100-SXM5-80GB`, `A100-SXM4-80GB`, `RTX-4090` |

**Response (per row):**
```json
{
  "hour_bucket":      "2024-01-14 18:00:00",
  "hour_of_day":      18,
  "gpu_type":         "H100-SXM5-80GB",
  "gpu_tier":         "flagship",
  "avg_util_pct":     87.4,
  "p95_util_pct":     94.1,
  "active_gpus":      128,
  "high_util_gpus":   89,
  "idle_gpus":        2,
  "total_power_kw":   62.3,
  "thermal_warnings": 1
}
```

#### `GET /utilization/capacity`

Multi-day capacity pressure summary by GPU type.

| Query param | Type | Default |
|---|---|---|
| `days` | int (1–30) | 7 |

**Response (per GPU type):**
```json
{
  "gpu_type":              "H100-SXM5-80GB",
  "gpu_tier":              "flagship",
  "avg_util_pct":          82.1,
  "peak_p95_util_pct":     97.8,
  "avg_idle_rate":         0.014,
  "avg_high_util_rate":    0.61,
  "total_thermal_warnings": 19,
  "capacity_pressure":     "medium"
}
```

`capacity_pressure` is bucketed by `avg_util_pct`: `> 85 → high`, `> 65 → medium`, else `low`.

---

### SLA / performance

#### `GET /sla/models`

p50 / p95 / p99 latency by model and region for a single date.

| Query param | Type | Default | Description |
|---|---|---|---|
| `query_date` | date | yesterday | |
| `breaches_only` | bool | false | If true, return only models where `slo_p99_breached = TRUE` |

**Response (per model/region):**
```json
{
  "model_id":         "llama-3-70b-instruct",
  "region":           "us-east-1",
  "size_tier":        "large",
  "total_requests":   31420,
  "cache_hit_rate":   0.221,
  "latency": {
    "p50_ms": 612.3,
    "p95_ms": 1840.5,
    "p99_ms": 4870.1,
    "p99_ms_non_cached": 5103.7
  },
  "slo_breached": false,
  "total_cost_usd": 28.47
}
```

#### `GET /sla/trends`

p99 latency trend for a single model over time.

| Query param | Type | Default | Description |
|---|---|---|---|
| `model_id` | string | required | e.g. `llama-3-70b-instruct` |
| `region` | string | (all) | Optional filter |
| `days` | int (1–90) | 14 | Lookback window |

**Response (per day):**
```json
{
  "date":           "2024-01-15",
  "region":         "us-east-1",
  "total_requests": 31420,
  "p50_ms":         612.3,
  "p99_ms":         4870.1,
  "cache_hit_rate": 0.221,
  "slo_breached":   false
}
```

---

## Error handling

All endpoints return JSON. Validation errors (e.g. `days` out of range) return 422 with FastAPI's standard validation envelope. Underlying DuckDB errors propagate as 500 — there is no retry layer in the API itself; the caller is expected to retry idempotent reads.

## Concurrency model

Each Uvicorn worker (default 2) holds an independent `:memory:` DuckDB connection. There is no shared state between workers and no shared file lock — workers only read Delta files from MinIO, which Delta Lake's transaction log permits at unbounded concurrency. Adding workers scales read throughput linearly until MinIO saturates.

If the API ever needed to *write* (e.g. cache aggregations in a shared DuckDB file), single-writer would become a bottleneck. At that point the right move is to add a Postgres or DuckDB-on-MotherDuck cache, not to share a `.duckdb` file.
