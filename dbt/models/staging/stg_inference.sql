{{
  config(
    materialized = 'view',
    description  = 'Staged inference API logs — casts and basic filtering'
  )
}}

SELECT
    request_id,
    org_id,
    user_id,
    model_id,
    region,

    CAST(status_code   AS INTEGER) AS status_code,
    CAST(cache_hit     AS BOOLEAN) AS cache_hit,
    CAST(is_success    AS BOOLEAN) AS is_success,

    -- Token counts
    CAST(input_tokens  AS INTEGER) AS input_tokens,
    CAST(output_tokens AS INTEGER) AS output_tokens,
    CAST(total_tokens  AS INTEGER) AS total_tokens,

    -- Performance
    CAST(latency_ms    AS DOUBLE)  AS latency_ms,
    CAST(cost_usd      AS DOUBLE)  AS cost_usd,

    CAST(timestamp     AS TIMESTAMP) AS requested_at,
    CAST(log_date      AS DATE)      AS log_date,
    CAST(log_hour      AS INTEGER)   AS log_hour

FROM delta_scan('{{ var("silver_path") }}/inference')

WHERE timestamp IS NOT NULL
