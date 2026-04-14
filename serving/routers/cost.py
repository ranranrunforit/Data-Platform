"""Cost attribution endpoints."""

from datetime import date, timedelta
from typing import Optional

import duckdb
from fastapi import APIRouter, Query, Request

router = APIRouter()


@router.get("/orgs")
def cost_by_org(
    request: Request,
    query_date: Optional[date] = Query(default=None, description="Date (YYYY-MM-DD). Defaults to yesterday."),
    days: int = Query(default=1, ge=1, le=90, description="Number of days to aggregate"),
):
    """
    GPU cost attribution aggregated by organisation.

    Returns total spend, GPU hours consumed, job counts, and OOM rate
    per org for the specified date range.
    """
    db: duckdb.DuckDBPyConnection = request.app.state.db
    gold = request.app.state.gold_path
    end_date = query_date or (date.today() - timedelta(days=1))
    start_date = end_date - timedelta(days=days - 1)

    rows = db.execute(f"""
        SELECT
            org_id,
            ROUND(SUM(total_cost_usd), 4)   AS total_cost_usd,
            ROUND(SUM(total_gpu_hours), 2)  AS total_gpu_hours,
            SUM(total_jobs)                 AS total_jobs,
            SUM(succeeded_jobs)             AS succeeded_jobs,
            SUM(oom_jobs)                   AS oom_jobs,
            ROUND(AVG(oom_rate), 4)         AS avg_oom_rate,
            ROUND(AVG(success_rate), 4)     AS avg_success_rate
        FROM delta_scan('{gold}/cost_attribution')
        WHERE job_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY org_id
        ORDER BY total_cost_usd DESC
    """).fetchall()

    return {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "orgs": [
            {
                "org_id": r[0],
                "total_cost_usd": r[1],
                "total_gpu_hours": r[2],
                "total_jobs": r[3],
                "succeeded_jobs": r[4],
                "oom_jobs": r[5],
                "avg_oom_rate": r[6],
                "avg_success_rate": r[7],
            }
            for r in rows
        ],
    }


@router.get("/models")
def cost_by_model_arch(
    request: Request,
    query_date: Optional[date] = Query(default=None),
    days: int = Query(default=7, ge=1, le=90),
):
    """Cost breakdown by model architecture — shows which workloads are most expensive."""
    db: duckdb.DuckDBPyConnection = request.app.state.db
    gold = request.app.state.gold_path
    end_date = query_date or (date.today() - timedelta(days=1))
    start_date = end_date - timedelta(days=days - 1)

    rows = db.execute(f"""
        SELECT
            model_arch,
            gpu_tier,
            ROUND(SUM(total_cost_usd), 4)       AS total_cost_usd,
            ROUND(SUM(total_gpu_hours), 2)      AS total_gpu_hours,
            SUM(total_jobs)                     AS total_jobs,
            ROUND(AVG(avg_cost_per_success_usd), 4) AS avg_cost_per_job,
            ROUND(AVG(avg_duration_hours), 4)   AS avg_duration_hours
        FROM delta_scan('{gold}/cost_attribution')
        WHERE job_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY model_arch, gpu_tier
        ORDER BY total_cost_usd DESC
    """).fetchall()

    return {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "models": [
            {
                "model_arch": r[0],
                "gpu_tier": r[1],
                "total_cost_usd": r[2],
                "total_gpu_hours": r[3],
                "total_jobs": r[4],
                "avg_cost_per_job": r[5],
                "avg_duration_hours": r[6],
            }
            for r in rows
        ],
    }
