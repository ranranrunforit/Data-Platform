{{
  config(
    materialized = 'view',
    description  = 'Staged GPU training jobs — casts, renames, and light filtering only'
  )
}}

/*
  Reads directly from the Delta table via DuckDB delta_scan().
  No business logic here — that lives in intermediate and mart models.
  The silver_path var is set in dbt_project.yml and overridable per environment.
*/

SELECT
    job_id,
    org_id,
    user_id,

    -- GPU hardware
    gpu_type,
    CAST(gpu_count AS INTEGER)          AS gpu_count,
    CAST(price_per_gpu_hour AS DOUBLE)  AS price_per_gpu_hour,

    -- Job metadata
    framework,
    model_arch,
    CAST(dataset_size_gb AS DOUBLE)     AS dataset_size_gb,

    -- Timestamps (cast to TIMESTAMP for DuckDB compatibility)
    CAST(started_at AS TIMESTAMP)       AS started_at,
    CAST(ended_at   AS TIMESTAMP)       AS ended_at,

    -- Derived duration + cost
    CAST(duration_hours AS DOUBLE)      AS duration_hours,
    CAST(gpu_hours      AS DOUBLE)      AS gpu_hours,
    CAST(cost_usd       AS DOUBLE)      AS cost_usd,

    -- Outcomes
    exit_code,
    CAST(is_success     AS BOOLEAN)     AS is_success,
    CAST(is_late_arrival AS BOOLEAN)    AS is_late_arrival,

    -- Partitioning / filtering helpers
    CAST(job_date AS DATE)              AS job_date,
    CAST(_ingested_at AS TIMESTAMP)     AS _ingested_at

FROM delta_scan('{{ var("silver_path") }}/jobs')

-- Exclude jobs with no start time (data quality guard)
WHERE started_at IS NOT NULL
