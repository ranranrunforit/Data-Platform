-- Test: no negative cost values in cost_attribution
-- A negative cost_usd would indicate a billing calculation bug.
-- This test FAILS (returns rows) if any negative costs exist.

SELECT
    attribution_id,
    org_id,
    job_date,
    total_cost_usd
FROM {{ ref('cost_attribution') }}
WHERE total_cost_usd < 0
