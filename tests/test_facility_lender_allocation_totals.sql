-- Test that allocation percentages for each facility sum to 100%
-- This test ensures data integrity for facility-lender allocations

WITH facility_allocation_totals AS (
  SELECT 
    facility_id,
    SUM(allocation_pct) as total_allocation_pct
  FROM {{ ref('facility_lender') }}
  GROUP BY facility_id
),

invalid_allocations AS (
  SELECT 
    facility_id,
    total_allocation_pct
  FROM facility_allocation_totals
  WHERE ABS(total_allocation_pct - 100.0) > 0.01  -- Allow for small rounding differences
)

SELECT * FROM invalid_allocations