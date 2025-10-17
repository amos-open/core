-- Test that each instrument has exactly one primary country for each valid period
-- This ensures primary flag consistency across temporal allocations

WITH instrument_country_primary AS (
  SELECT 
    instrument_id,
    valid_from,
    COALESCE(valid_to, '9999-12-31'::date) as valid_to_coalesced,
    SUM(CASE WHEN primary_flag = TRUE THEN 1 ELSE 0 END) as primary_count
  FROM {{ ref('instrument_country') }}
  GROUP BY 
    instrument_id, 
    valid_from, 
    COALESCE(valid_to, '9999-12-31'::date)
),

invalid_primary_flags AS (
  SELECT 
    instrument_id,
    valid_from,
    valid_to_coalesced,
    primary_count
  FROM instrument_country_primary
  WHERE primary_count != 1  -- Should have exactly one primary country per period
)

SELECT *
FROM invalid_primary_flags