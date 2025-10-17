-- Test to validate instrument_cashflow_type enum values match DBML specification
-- This test ensures all cashflow types are within the accepted values

WITH invalid_cashflow_types AS (
  SELECT 
    id,
    instrument_id,
    cashflow_type,
    'Invalid cashflow_type value' as error_message
  FROM {{ ref('instrument_cashflow') }}
  WHERE cashflow_type NOT IN (
    'CONTRIBUTION',
    'DISTRIBUTION', 
    'DIVIDEND',
    'INTEREST',
    'FEE',
    'PRINCIPAL',
    'DRAW',
    'PREPAYMENT',
    'OTHER'
  )
),

cashflow_type_distribution AS (
  SELECT 
    cashflow_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
  FROM {{ ref('instrument_cashflow') }}
  GROUP BY cashflow_type
)

-- Return any invalid cashflow types
SELECT 
  id,
  instrument_id,
  cashflow_type,
  error_message
FROM invalid_cashflow_types

UNION ALL

-- Also check for any missing expected cashflow types in test data (informational)
SELECT 
  NULL as id,
  NULL as instrument_id,
  expected_type as cashflow_type,
  'Expected cashflow type not found in data' as error_message
FROM (
  VALUES 
    ('CONTRIBUTION'),
    ('DISTRIBUTION'),
    ('DIVIDEND'),
    ('INTEREST'),
    ('FEE'),
    ('PRINCIPAL'),
    ('DRAW'),
    ('PREPAYMENT'),
    ('OTHER')
) AS expected(expected_type)
WHERE expected_type NOT IN (
  SELECT DISTINCT cashflow_type 
  FROM {{ ref('instrument_cashflow') }}
  WHERE cashflow_type IS NOT NULL
)
AND (SELECT COUNT(*) FROM {{ ref('instrument_cashflow') }}) > 0  -- Only check if we have data