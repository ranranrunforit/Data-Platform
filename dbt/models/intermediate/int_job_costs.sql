{{
  config(
    materialized = 'table',
    description  = 'Job-level cost enrichment — handles partial hours and spot interruptions'
  )
}}

/*
  Business logic for cost calculation lives here, not in staging.

  Key considerations for an AI cloud billing model:
    1. Partial hours: billed by the second (duration / 3600 * hourly_rate)
    2. OOM kills (exit_code=137): still billed — customer used the GPU
    3. Platform errors (exit_code=143): not billed — our fault
    4. Still-running jobs (ended_at IS NULL): billed up to current time as "accrued"
*/

WITH base AS (
    SELECT * FROM {{ ref('stg_jobs') }}
),

cost_enriched AS (
    SELECT
        job_id,
        org_id,
        user_id,
        gpu_type,
        gpu_count,
        framework,
        model_arch,
        dataset_size_gb,
        started_at,
        ended_at,
        exit_code,
        is_success,
        is_late_arrival,
        job_date,

        -- Effective duration:
        -- completed jobs → actual; in-flight → time so far; platform errors → 0
        CASE
            WHEN exit_code = 143 THEN 0.0                              -- platform fault: no charge
            WHEN ended_at IS NOT NULL THEN duration_hours
            ELSE EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) / 3600.0
        END AS billable_hours,

        gpu_hours,
        price_per_gpu_hour,

        -- Recalculate cost using billable_hours (not raw cost_usd from Silver,
        -- which may be NULL for late-arriving jobs)
        CASE
            WHEN exit_code = 143 THEN 0.0
            WHEN ended_at IS NOT NULL
                THEN ROUND(duration_hours * gpu_count * price_per_gpu_hour, 6)
            ELSE
                ROUND(
                    (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at)) / 3600.0)
                    * gpu_count * price_per_gpu_hour,
                    6
                )
        END AS cost_usd,

        -- Status labels for dashboards
        CASE
            WHEN ended_at IS NULL     THEN 'running'
            WHEN exit_code = 0        THEN 'succeeded'
            WHEN exit_code = 137      THEN 'oom_killed'
            WHEN exit_code = 143      THEN 'platform_error'
            ELSE 'failed'
        END AS job_status,

        -- GPU tier (for analytics grouping)
        CASE
            WHEN gpu_type LIKE 'H100%' THEN 'flagship'
            WHEN gpu_type LIKE 'A100%' THEN 'performance'
            ELSE 'standard'
        END AS gpu_tier

    FROM base
)

SELECT * FROM cost_enriched
