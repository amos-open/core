-- Test that commitment amounts are consistent with allocation percentages
-- This test ensures that the sum of commitment amounts matches the facility total commitment

WITH facility_totals AS (
  SELECT 
    f.id as facility_id,
    f.total_commitment as facility_total_commitment,
    SUM(fl.commitment_amount) as sum_lender_commitments,
    SUM(fl.allocation_pct) as sum_allocation_pct
  FROM {{ ref('facility') }} f
  JOIN {{ ref('facility_lender') }} fl ON f.id = fl.facility_id
  GROUP BY f.id, f.total_commitment
),

inconsistent_commitments AS (
  SELECT 
    facility_id,
    facility_total_commitment,
    sum_lender_commitments,
    sum_allocation_pct,
    ABS(facility_total_commitment - sum_lender_commitments) as commitment_difference
  FROM facility_totals
  WHERE ABS(facility_total_commitment - sum_lender_commitments) > 0.01  -- Allow for small rounding differences
)

SELECT * FROM inconsistent_commitments