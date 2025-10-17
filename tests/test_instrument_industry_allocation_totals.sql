-- Test that allocation percentages for each instrument sum to 100% for each valid period
-- This test ensures temporal allocation consistency for industry allocations

WITH instrument_industry_allocations AS (
  SELECT 
    instrument_id,
    valid_from,
    COALESCE(valid_to, '9999-12-31'::date) as valid_to_coalesced,
    SUM(allocation_pct) as total_allocation_pct
  FROM {{ ref('instrument_industry') }}
  GROUP BY 
    instrument_id, 
    valid_from, 
    COALESCE(valid_to, '9999-12-31'::date)
),

invalid_allocations AS (
  SELECT 
    instrument_id,
    valid_from,
    valid_to_coalesced,
    total_allocation_pct
  FROM instrument_industry_allocations
  WHERE ABS(total_allocation_pct - 100.0) > 0.01  -- Allow for small rounding differences
)

SELECT *
FROM invalid_allocations