-- Test that allocation percentages for each facility sum to 100% at any point in time
-- This test ensures data integrity for time-variant facility-country allocations

WITH facility_date_ranges AS (
  -- Get all unique dates where allocations change
  SELECT DISTINCT 
    facility_id,
    valid_from as allocation_date
  FROM {{ ref('facility_country') }}
  
  UNION
  
  SELECT DISTINCT 
    facility_id,
    valid_to as allocation_date
  FROM {{ ref('facility_country') }}
  WHERE valid_to IS NOT NULL
),

facility_allocations_by_date AS (
  SELECT 
    fdr.facility_id,
    fdr.allocation_date,
    SUM(fc.allocation_pct) as total_allocation_pct
  FROM facility_date_ranges fdr
  JOIN {{ ref('facility_country') }} fc 
    ON fdr.facility_id = fc.facility_id
    AND fdr.allocation_date >= fc.valid_from
    AND (fc.valid_to IS NULL OR fdr.allocation_date < fc.valid_to)
  GROUP BY fdr.facility_id, fdr.allocation_date
),

invalid_allocations AS (
  SELECT 
    facility_id,
    allocation_date,
    total_allocation_pct
  FROM facility_allocations_by_date
  WHERE ABS(total_allocation_pct - 100.0) > 0.01  -- Allow for small rounding differences
)

SELECT * FROM invalid_allocations