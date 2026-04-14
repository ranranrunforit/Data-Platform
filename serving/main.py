"""
FastAPI serving layer — DuckDB reads Gold Delta tables directly.

Why DuckDB here instead of a traditional warehouse:
  - Zero-copy: reads Delta files from MinIO without importing data
  - Serverless: no additional DB to manage
  - Fast enough: sub-second analytical queries on 10s of millions of rows
  - Same Delta files used by dbt and Spark — one source of truth

Endpoints:
  GET /health                       → liveness check
  GET /cost/orgs?date=2024-01-15    → cost attribution by org
  GET /utilization/hourly?date=...  → GPU utilization by type
  GET /sla/models?date=...          → p99 latency + SLO breach flags
  GET /metrics/summary              → dashboard summary (last 7 days)
"""

import os
from contextlib import asynccontextmanager

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import cost, sla, utilization

GOLD_PATH = os.getenv("GOLD_PATH", "s3://gold")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")


def _build_connection() -> duckdb.DuckDBPyConnection:
    """
    Build a DuckDB connection configured to read from MinIO (S3-compatible).
    The httpfs + delta extensions enable direct Delta Lake reads.

    Concurrency note: each connection is in-memory (:memory:), so multiple
    API workers each get an independent DuckDB instance. There is no shared
    file lock — workers only read Delta files from MinIO (read-only by nature).
    Delta Lake supports unlimited concurrent readers safely.

    Ceiling: if this API needed to *write* results back (e.g., caching
    aggregations in a shared DuckDB file), single-writer would become a
    bottleneck. At that point, replace with MotherDuck or a Postgres
    materialization layer. For a read-only analytics API this is fine.
    """
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{AWS_KEY}';
        SET s3_secret_access_key='{AWS_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: build shared DuckDB connection
    app.state.db = _build_connection()
    app.state.gold_path = GOLD_PATH
    print(f"DuckDB ready. Reading Gold from: {GOLD_PATH}")
    yield
    # Shutdown: close connection
    app.state.db.close()


app = FastAPI(
    title="AI Data Platform API",
    description=(
        "Analytics API for AI infrastructure telemetry. "
        "Reads Gold Delta Lake tables via DuckDB. "
        "Source: 96-node GPU cluster, 500K+ daily inference requests."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(cost.router,        prefix="/cost",        tags=["Cost Attribution"])
app.include_router(utilization.router, prefix="/utilization", tags=["GPU Utilization"])
app.include_router(sla.router,         prefix="/sla",         tags=["SLA / Performance"])


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics/summary")
def summary(app_state=None):
    """7-day dashboard summary — cost, utilization, SLO compliance."""
    db: duckdb.DuckDBPyConnection = app.state.db
    gold = app.state.gold_path

    row = db.execute(f"""
        SELECT
            ROUND(SUM(total_cost_usd), 2)   AS total_cost_7d,
            ROUND(SUM(total_gpu_hours), 1)  AS total_gpu_hours_7d,
            SUM(total_jobs)                 AS total_jobs_7d,
            ROUND(AVG(success_rate), 4)     AS avg_success_rate,
            ROUND(AVG(oom_rate), 4)         AS avg_oom_rate
        FROM delta_scan('{gold}/cost_attribution')
        WHERE job_date >= CURRENT_DATE - INTERVAL 7 DAYS
    """).fetchone()

    slo_row = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE slo_p99_breached) AS slo_breaches_7d,
            COUNT(*)                                 AS total_model_days_7d
        FROM delta_scan('{gold}/job_performance_sla')
        WHERE log_date >= CURRENT_DATE - INTERVAL 7 DAYS
    """).fetchone()

    return {
        "period": "last_7_days",
        "cost": {
            "total_usd": row[0],
            "total_gpu_hours": row[1],
        },
        "jobs": {
            "total": row[2],
            "avg_success_rate": row[3],
            "avg_oom_rate": row[4],
        },
        "slo": {
            "breach_count": slo_row[0],
            "total_model_days": slo_row[1],
            "compliance_rate": round(
                1 - (slo_row[0] / max(slo_row[1], 1)), 4
            ),
        },
    }
