-- Test that allocation percentages for each company sum to 100%
-- This test ensures data integrity for company-country allocations

WITH company_allocation_totals AS (
  SELECT 
    company_id,
    SUM(allocation_pct) as total_allocation_pct
  FROM {{ ref('company_country') }}
  GROUP BY company_id
),

invalid_allocations AS (
  SELECT 
    company_id,
    total_allocation_pct
  FROM company_allocation_totals
  WHERE ABS(total_allocation_pct - 100.0) > 0.01  -- Allow for small rounding differences
)

SELECT * FROM invalid_allocations