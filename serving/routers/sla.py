"""Inference SLA / performance endpoints."""

from datetime import date, timedelta
from typing import Optional

import duckdb
from fastapi import APIRouter, Query, Request

router = APIRouter()


@router.get("/models")
def sla_by_model(
    request: Request,
    query_date: Optional[date] = Query(default=None),
    breaches_only: bool = Query(default=False, description="Return only SLO-breaching models"),
):
    """
    p50 / p95 / p99 inference latency per model per region for a given date.
    Pre-computed SLO breach flag indicates whether the p99 threshold was exceeded.
    """
    db: duckdb.DuckDBPyConnection = request.app.state.db
    gold = request.app.state.gold_path
    query_date = query_date or (date.today() - timedelta(days=1))

    breach_filter = "AND slo_p99_breached = TRUE" if breaches_only else ""

    rows = db.execute(f"""
        SELECT
            model_id,
            region,
            model_size_tier,
            total_requests,
            ROUND(cache_hit_rate, 4)              AS cache_hit_rate,
            ROUND(p50_latency_ms, 1)              AS p50_ms,
            ROUND(p95_latency_ms, 1)              AS p95_ms,
            ROUND(p99_latency_ms, 1)              AS p99_ms,
            ROUND(p99_latency_ms_non_cached, 1)   AS p99_ms_non_cached,
            slo_p99_breached,
            ROUND(total_cost_usd, 4)              AS total_cost_usd
        FROM delta_scan('{gold}/job_performance_sla')
        WHERE log_date = '{query_date}'
        {breach_filter}
        ORDER BY slo_p99_breached DESC, p99_latency_ms DESC
    """).fetchall()

    return {
        "date": str(query_date),
        "breaches_only": breaches_only,
        "models": [
            {
                "model_id": r[0],
                "region": r[1],
                "size_tier": r[2],
                "total_requests": r[3],
                "cache_hit_rate": r[4],
                "latency": {
                    "p50_ms": r[5],
                    "p95_ms": r[6],
                    "p99_ms": r[7],
                    "p99_ms_non_cached": r[8],
                },
                "slo_breached": r[9],
                "total_cost_usd": r[10],
            }
            for r in rows
        ],
    }


@router.get("/trends")
def sla_trends(
    request: Request,
    model_id: str = Query(..., description="Model ID to trend"),
    region: Optional[str] = Query(default=None),
    days: int = Query(default=14, ge=1, le=90),
):
    """
    p99 latency trend for a specific model over time.
    Used for spotting gradual degradation before it breaches the SLO.
    """
    db: duckdb.DuckDBPyConnection = request.app.state.db
    gold = request.app.state.gold_path

    region_filter = f"AND region = '{region}'" if region else ""

    rows = db.execute(f"""
        SELECT
            log_date,
            region,
            total_requests,
            ROUND(p50_latency_ms, 1)  AS p50_ms,
            ROUND(p99_latency_ms, 1)  AS p99_ms,
            cache_hit_rate,
            slo_p99_breached
        FROM delta_scan('{gold}/job_performance_sla')
        WHERE model_id = '{model_id}'
          AND log_date >= CURRENT_DATE - INTERVAL {days} DAYS
          {region_filter}
        ORDER BY log_date
    """).fetchall()

    return {
        "model_id": model_id,
        "region": region,
        "period_days": days,
        "trend": [
            {
                "date": str(r[0]),
                "region": r[1],
                "total_requests": r[2],
                "p50_ms": r[3],
                "p99_ms": r[4],
                "cache_hit_rate": r[5],
                "slo_breached": r[6],
            }
            for r in rows
        ],
    }
