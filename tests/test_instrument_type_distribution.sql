-- Test to validate instrument_type distribution and ensure no invalid values
-- This test will fail if there are any instrument_type values outside the accepted list
-- Validates the unified instrument model's type constraints

WITH instrument_type_check AS (
  SELECT 
    instrument_type,
    COUNT(*) as count
  FROM {{ ref('instrument') }}
  WHERE instrument_type NOT IN ('EQUITY', 'LOAN', 'CONVERTIBLE', 'WARRANT', 'FUND_INTEREST')
  GROUP BY instrument_type
),

instrument_type_stats AS (
  SELECT 
    instrument_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
  FROM {{ ref('instrument') }}
  GROUP BY instrument_type
)

-- Return invalid instrument types (should be empty)
SELECT 
  instrument_type,
  count,
  'Invalid instrument_type found' as error_message
FROM instrument_type_check

UNION ALL

-- Also validate that we have reasonable distribution (optional check)
SELECT 
  instrument_type,
  count,
  'Instrument type distribution check' as error_message
FROM instrument_type_stats
WHERE count = 0  -- This would indicate missing instrument types in test data