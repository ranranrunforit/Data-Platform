{{
  config(
    materialized = 'table',
    description  = 'Hourly GPU utilization aggregated from node metrics — used for capacity planning'
  )
}}

/*
  Reads node metrics from Silver Delta table.
  Grain: one row per (hour, gpu_type, gpu_tier)

  Capacity planning queries this to answer:
    - Which GPU type is consistently > 90% utilized? (needs more capacity)
    - Which hours have troughs? (schedule maintenance then)
    - Which rack is thermal-throttling? (power management)
*/

WITH raw_metrics AS (
    SELECT
        CAST(timestamp AS TIMESTAMP)          AS metric_ts,
        DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour_bucket,
        CAST(DATE(timestamp) AS DATE)         AS metric_date,
        node_id,
        gpu_id,
        rack_id,
        gpu_type,

        CASE
            WHEN gpu_type LIKE 'H100%' THEN 'flagship'
            WHEN gpu_type LIKE 'A100%' THEN 'performance'
            ELSE 'standard'
        END AS gpu_tier,

        CAST(gpu_util_pct    AS DOUBLE)       AS gpu_util_pct,
        CAST(memory_util_pct AS DOUBLE)       AS memory_util_pct,
        CAST(temp_celsius    AS DOUBLE)       AS temp_celsius,
        CAST(power_watts     AS DOUBLE)       AS power_watts

    FROM delta_scan('{{ var("silver_path") }}/node_metrics')
    WHERE timestamp IS NOT NULL
),

hourly_agg AS (
    SELECT
        hour_bucket,
        metric_date,
        gpu_type,
        gpu_tier,
        rack_id,

        -- GPU utilization distribution
        ROUND(AVG(gpu_util_pct), 2)                          AS avg_gpu_util_pct,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
              (ORDER BY gpu_util_pct), 2)                    AS p50_gpu_util_pct,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
              (ORDER BY gpu_util_pct), 2)                    AS p95_gpu_util_pct,
        ROUND(MAX(gpu_util_pct), 2)                          AS max_gpu_util_pct,

        -- Memory utilization
        ROUND(AVG(memory_util_pct), 2)                       AS avg_memory_util_pct,
        ROUND(MAX(memory_util_pct), 2)                       AS max_memory_util_pct,

        -- Thermal + power
        ROUND(AVG(temp_celsius), 1)                          AS avg_temp_celsius,
        ROUND(MAX(temp_celsius), 1)                          AS max_temp_celsius,
        ROUND(SUM(power_watts) / 1000.0, 2)                 AS total_power_kw,

        -- Capacity signals
        COUNT(DISTINCT node_id)                              AS active_nodes,
        COUNT(DISTINCT gpu_id)                               AS active_gpus,
        COUNT(*) FILTER (WHERE gpu_util_pct > 90)           AS high_util_gpu_count,
        COUNT(*) FILTER (WHERE gpu_util_pct < 10)           AS idle_gpu_count,
        COUNT(*) FILTER (WHERE temp_celsius > 85)           AS thermal_warning_count

    FROM raw_metrics
    GROUP BY hour_bucket, metric_date, gpu_type, gpu_tier, rack_id
)

SELECT
    hour_bucket,
    metric_date,
    EXTRACT(HOUR FROM hour_bucket)                           AS hour_of_day,
    EXTRACT(DOW FROM hour_bucket)                           AS day_of_week,
    gpu_type,
    gpu_tier,
    rack_id,
    avg_gpu_util_pct,
    p50_gpu_util_pct,
    p95_gpu_util_pct,
    max_gpu_util_pct,
    avg_memory_util_pct,
    max_memory_util_pct,
    avg_temp_celsius,
    max_temp_celsius,
    total_power_kw,
    active_nodes,
    active_gpus,
    high_util_gpu_count,
    idle_gpu_count,
    thermal_warning_count,
    -- Capacity utilization rate: what % of GPUs were high-utilisation
    ROUND(high_util_gpu_count::DOUBLE / NULLIF(active_gpus, 0), 4) AS high_util_rate,
    ROUND(idle_gpu_count::DOUBLE     / NULLIF(active_gpus, 0), 4) AS idle_rate

FROM hourly_agg
ORDER BY hour_bucket DESC
