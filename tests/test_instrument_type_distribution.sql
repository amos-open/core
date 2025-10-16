-- Test to validate instrument_type distribution and ensure no invalid values
-- This test will fail if there are any instrument_type values outside the accepted list

WITH instrument_type_check AS (
  SELECT 
    instrument_type,
    COUNT(*) as count
  FROM {{ ref('instrument') }}
  WHERE instrument_type NOT IN ('EQUITY', 'LOAN', 'CONVERTIBLE', 'WARRANT', 'FUND_INTEREST')
  GROUP BY instrument_type
)

SELECT 
  instrument_type,
  count
FROM instrument_type_check