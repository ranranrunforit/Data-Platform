"""GPU utilization endpoints."""

from datetime import date, timedelta
from typing import Optional

import duckdb
from fastapi import APIRouter, Query, Request

router = APIRouter()


@router.get("/hourly")
def utilization_hourly(
    request: Request,
    query_date: Optional[date] = Query(default=None),
    gpu_type: Optional[str] = Query(default=None, description="Filter by GPU type"),
):
    """
    Hourly GPU utilization for a given date.
    Used for capacity planning — identify overloaded and underutilised windows.
    """
    db: duckdb.DuckDBPyConnection = request.app.state.db
    gold = request.app.state.gold_path
    query_date = query_date or (date.today() - timedelta(days=1))

    gpu_filter = f"AND gpu_type = '{gpu_type}'" if gpu_type else ""

    rows = db.execute(f"""
        SELECT
            hour_bucket,
            hour_of_day,
            gpu_type,
            gpu_tier,
            ROUND(avg_gpu_util_pct, 2)   AS avg_util_pct,
            ROUND(p95_gpu_util_pct, 2)   AS p95_util_pct,
            active_gpus,
            high_util_gpu_count,
            idle_gpu_count,
            ROUND(total_power_kw, 2)     AS total_power_kw,
            thermal_warning_count
        FROM delta_scan('{gold}/gpu_utilization_hourly')
        WHERE metric_date = '{query_date}'
        {gpu_filter}
        ORDER BY hour_bucket, gpu_type
    """).fetchall()

    return {
        "date": str(query_date),
        "gpu_type_filter": gpu_type,
        "hours": [
            {
                "hour_bucket": str(r[0]),
                "hour_of_day": r[1],
                "gpu_type": r[2],
                "gpu_tier": r[3],
                "avg_util_pct": r[4],
                "p95_util_pct": r[5],
                "active_gpus": r[6],
                "high_util_gpus": r[7],
                "idle_gpus": r[8],
                "total_power_kw": r[9],
                "thermal_warnings": r[10],
            }
            for r in rows
        ],
    }


@router.get("/capacity")
def capacity_summary(
    request: Request,
    days: int = Query(default=7, ge=1, le=30),
):
    """
    7-day capacity summary by GPU type.
    Answers: which GPU types are consistently over 85% — need more nodes?
    """
    db: duckdb.DuckDBPyConnection = request.app.state.db
    gold = request.app.state.gold_path

    rows = db.execute(f"""
        SELECT
            gpu_type,
            gpu_tier,
            ROUND(AVG(avg_gpu_util_pct), 2)   AS avg_util_7d,
            ROUND(MAX(p95_gpu_util_pct), 2)   AS peak_p95_util,
            ROUND(AVG(idle_rate), 4)           AS avg_idle_rate,
            ROUND(AVG(high_util_rate), 4)      AS avg_high_util_rate,
            SUM(thermal_warning_count)         AS total_thermal_warnings
        FROM delta_scan('{gold}/gpu_utilization_hourly')
        WHERE metric_date >= CURRENT_DATE - INTERVAL {days} DAYS
        GROUP BY gpu_type, gpu_tier
        ORDER BY avg_util_7d DESC
    """).fetchall()

    return {
        "period_days": days,
        "gpu_types": [
            {
                "gpu_type": r[0],
                "gpu_tier": r[1],
                "avg_util_pct": r[2],
                "peak_p95_util_pct": r[3],
                "avg_idle_rate": r[4],
                "avg_high_util_rate": r[5],
                "total_thermal_warnings": r[6],
                "capacity_pressure": "high" if r[2] > 85 else "medium" if r[2] > 65 else "low",
            }
            for r in rows
        ],
    }
