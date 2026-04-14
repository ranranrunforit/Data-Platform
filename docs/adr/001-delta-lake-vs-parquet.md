# ADR-001: Delta Lake over Plain Parquet

**Status:** Accepted  
**Date:** 2024-01-15  
**Deciders:** Data Engineering

---

## Context

The Silver layer must support:
1. **Upserts** — job completion events arrive hours after job start. We need to update existing rows, not append duplicates.
2. **Idempotent writes** — Airflow may retry a failed task. Re-running the Spark job must not create duplicate records.
3. **Schema evolution** — new GPU types and framework names will be added over time without breaking existing queries.
4. **Time-travel** — if a dbt model produces incorrect results, we need to query the Silver table as it was at a point in time.

Plain Parquet satisfies none of these without significant custom code.

## Options Considered

| Option | MERGE/Upsert | ACID | Schema Evolution | Time Travel |
|---|---|---|---|---|
| Plain Parquet | ❌ (read-modify-write is fragile) | ❌ | ❌ (breaks readers) | ❌ |
| **Delta Lake** | ✅ | ✅ | ✅ (additive) | ✅ (7-day default) |
| Apache Iceberg | ✅ | ✅ | ✅ | ✅ |
| Apache Hudi | ✅ (COW/MOR) | ✅ | ✅ | ✅ |

## Decision

**Delta Lake.**

### Why Delta over Iceberg/Hudi

- **Ecosystem fit:** Databricks (who maintain Delta) is the dominant enterprise Spark platform. Delta knowledge transfers directly to 80% of enterprise DE jobs.
- **DuckDB integration:** DuckDB 0.10+ reads Delta files natively via `delta_scan()`. This lets us use dbt-duckdb for Gold models without a Spark Thrift Server — a significant operational simplification.
- **Operational simplicity:** Delta's transaction log is a directory of JSON files. Easy to inspect, debug, and back up. Iceberg has a more complex catalog requirement.
- **MERGE semantics:** Delta's MERGE API is well-documented and handles the late-arrival pattern cleanly (see `spark/jobs/bronze_to_silver.py`).

### Why not Iceberg

Iceberg is technically superior in multi-engine scenarios (Flink + Spark + Trino). If this platform needed Flink for streaming, Iceberg would be the better choice. For Spark-only workloads, Delta's simpler operational model wins.

## Consequences

- All Silver and Gold tables are Delta format stored in MinIO.
- DuckDB can read them directly for dbt and API queries — no data duplication.
- Z-ORDER OPTIMIZE runs nightly via Airflow to maintain read performance.
- Time-travel retention: 7 days (configurable via `delta.logRetentionDuration`).
- If we ever add Flink, migrate to Iceberg at that point.
