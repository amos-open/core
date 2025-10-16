-- Test that each company has exactly one primary industry
-- This test ensures data integrity for primary flag usage

WITH company_primary_counts AS (
  SELECT 
    company_id,
    COUNT(*) as primary_count
  FROM {{ ref('company_industry') }}
  WHERE primary_flag = TRUE
  GROUP BY company_id
),

invalid_primary_flags AS (
  SELECT 
    company_id,
    primary_count
  FROM company_primary_counts
  WHERE primary_count != 1
)

SELECT * FROM invalid_primary_flags