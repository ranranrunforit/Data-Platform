{{
  config(
    materialized = 'table',
    description  = 'Daily p50/p95/p99 inference latency by model and region — SLO monitoring mart'
  )
}}

/*
  SLA / SLO performance mart.

  AI cloud companies typically commit to:
    - p99 latency < 2000ms for standard models
    - p99 latency < 5000ms for large models (70B+)

  This model is queried by:
    - The streaming health Airflow DAG (15-min checks)
    - The API /sla/models endpoint
    - On-call engineers when a latency alert fires
*/

WITH inference AS (
    SELECT * FROM {{ ref('stg_inference') }}
    WHERE is_success = TRUE   -- only count successful requests in SLOs
),

daily_sla AS (
    SELECT
        log_date,
        model_id,
        region,

        -- Volume
        COUNT(*)                                              AS total_requests,
        COUNT(*) FILTER (WHERE cache_hit = TRUE)             AS cache_hit_requests,
        ROUND(
            COUNT(*) FILTER (WHERE cache_hit = TRUE)::DOUBLE
            / NULLIF(COUNT(*), 0), 4
        )                                                     AS cache_hit_rate,

        -- Latency distribution (all requests)
        ROUND(AVG(latency_ms), 2)                            AS avg_latency_ms,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
              (ORDER BY latency_ms), 2)                      AS p50_latency_ms,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
              (ORDER BY latency_ms), 2)                      AS p95_latency_ms,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP
              (ORDER BY latency_ms), 2)                      AS p99_latency_ms,
        ROUND(MAX(latency_ms), 2)                            AS max_latency_ms,

        -- Latency (non-cached only — fairer measure of generation speed)
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
              (ORDER BY latency_ms) FILTER (WHERE cache_hit = FALSE), 2)
                                                              AS p50_latency_ms_non_cached,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP
              (ORDER BY latency_ms) FILTER (WHERE cache_hit = FALSE), 2)
                                                              AS p99_latency_ms_non_cached,

        -- Token throughput
        ROUND(AVG(total_tokens), 0)                          AS avg_tokens_per_request,
        SUM(total_tokens)                                    AS total_tokens,
        ROUND(SUM(cost_usd), 4)                              AS total_cost_usd,
        ROUND(AVG(cost_usd), 8)                              AS avg_cost_per_request

    FROM inference
    GROUP BY log_date, model_id, region
),

with_slo AS (
    SELECT
        *,
        -- SLO breach flags (thresholds typical for AI inference SLAs)
        CASE
            WHEN model_id LIKE '%405b%' THEN p99_latency_ms > 8000
            WHEN model_id LIKE '%70b%'  THEN p99_latency_ms > 5000
            ELSE                             p99_latency_ms > 2000
        END AS slo_p99_breached,

        -- Classify model size tier for SLO threshold grouping
        CASE
            WHEN model_id LIKE '%405b%'          THEN 'xl'
            WHEN model_id LIKE '%70b%'           THEN 'large'
            WHEN model_id LIKE '%34b%'
              OR model_id LIKE '%mixtral%'       THEN 'medium'
            ELSE                                      'small'
        END AS model_size_tier

    FROM daily_sla
)

SELECT * FROM with_slo
ORDER BY log_date DESC, total_requests DESC
