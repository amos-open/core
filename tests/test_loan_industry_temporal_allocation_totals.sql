-- Test that allocation percentages for each loan sum to 100% at any point in time
-- This test ensures data integrity for time-variant loan-industry allocations

WITH loan_date_ranges AS (
  -- Get all unique dates where allocations change
  SELECT DISTINCT 
    loan_id,
    valid_from as allocation_date
  FROM {{ ref('loan_industry') }}
  
  UNION
  
  SELECT DISTINCT 
    loan_id,
    valid_to as allocation_date
  FROM {{ ref('loan_industry') }}
  WHERE valid_to IS NOT NULL
),

loan_allocations_by_date AS (
  SELECT 
    ldr.loan_id,
    ldr.allocation_date,
    SUM(li.allocation_pct) as total_allocation_pct
  FROM loan_date_ranges ldr
  JOIN {{ ref('loan_industry') }} li 
    ON ldr.loan_id = li.loan_id
    AND ldr.allocation_date >= li.valid_from
    AND (li.valid_to IS NULL OR ldr.allocation_date < li.valid_to)
  GROUP BY ldr.loan_id, ldr.allocation_date
),

invalid_allocations AS (
  SELECT 
    loan_id,
    allocation_date,
    total_allocation_pct
  FROM loan_allocations_by_date
  WHERE ABS(total_allocation_pct - 100.0) > 0.01  -- Allow for small rounding differences
)

SELECT * FROM invalid_allocations