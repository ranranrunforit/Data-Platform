{{
  config(
    materialized = 'table',
    description  = 'Daily GPU spend allocated by org → user → model architecture. The billing analytics mart.'
  )
}}

/*
  Cost attribution model.

  This is the differentiating mart for an AI cloud company:
  finance, customer success, and product teams all query this table.

  Grain: one row per (org_id, user_id, model_arch, gpu_tier, job_date)

  Aggregates:
    - Total spend and GPU hours consumed
    - Job counts by status (for SLA reporting)
    - Average cost and duration per successful job
    - OOM rate (signals customers who may need memory-optimised instances)
*/

WITH jobs AS (
    SELECT * FROM {{ ref('int_job_costs') }}
    WHERE job_date IS NOT NULL
),

daily_cost AS (
    SELECT
        job_date,
        org_id,
        user_id,
        model_arch,
        gpu_tier,
        gpu_type,
        framework,

        -- Volume
        COUNT(*)                                            AS total_jobs,
        COUNT(*) FILTER (WHERE job_status = 'succeeded')   AS succeeded_jobs,
        COUNT(*) FILTER (WHERE job_status = 'oom_killed')  AS oom_jobs,
        COUNT(*) FILTER (WHERE job_status = 'failed')      AS failed_jobs,
        COUNT(*) FILTER (WHERE job_status = 'running')     AS running_jobs,

        -- Cost
        ROUND(SUM(cost_usd), 4)                            AS total_cost_usd,
        ROUND(SUM(gpu_hours), 4)                           AS total_gpu_hours,
        ROUND(AVG(cost_usd) FILTER (
            WHERE job_status = 'succeeded'), 4)            AS avg_cost_per_success_usd,

        -- Duration (successful jobs only — failed jobs skew mean)
        ROUND(AVG(billable_hours) FILTER (
            WHERE job_status = 'succeeded'), 4)            AS avg_duration_hours,
        ROUND(MAX(billable_hours), 4)                      AS max_duration_hours,

        -- Rates
        ROUND(
            COUNT(*) FILTER (WHERE job_status = 'oom_killed')::DOUBLE
            / NULLIF(COUNT(*), 0),
        4)                                                 AS oom_rate,

        ROUND(
            COUNT(*) FILTER (WHERE job_status = 'succeeded')::DOUBLE
            / NULLIF(COUNT(*) FILTER (WHERE job_status != 'running'), 0),
        4)                                                 AS success_rate

    FROM jobs
    GROUP BY
        job_date, org_id, user_id, model_arch, gpu_tier, gpu_type, framework
)

SELECT
    -- Surrogate key (deterministic for dedup / idempotent writes)
    {{ dbt_utils.generate_surrogate_key([
        'job_date', 'org_id', 'user_id', 'model_arch', 'gpu_tier', 'gpu_type', 'framework'
    ]) }}                                                   AS attribution_id,
    *
FROM daily_cost
ORDER BY job_date DESC, total_cost_usd DESC
