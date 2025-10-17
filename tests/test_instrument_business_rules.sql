-- Test comprehensive business rules for the unified instrument model
-- Validates instrument-centric approach business logic

WITH instrument_validation AS (
  SELECT 
    i.id,
    i.instrument_type,
    i.company_id,
    i.fund_id,
    i.base_currency_code,
    i.inception_date,
    i.termination_date,
    f.name as fund_name,
    c.name as company_name
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('fund') }} f ON i.fund_id = f.id
  LEFT JOIN {{ ref('company') }} c ON i.company_id = c.id
),

business_rule_violations AS (
  -- Rule 1: Equity instruments must have company_id
  SELECT 
    id,
    instrument_type,
    'equity_missing_company_id' as violation_type,
    'Equity instruments must have company_id populated' as violation_message
  FROM instrument_validation
  WHERE instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
    AND company_id IS NULL

  UNION ALL

  -- Rule 2: Fund interest instruments should not have company_id
  SELECT 
    id,
    instrument_type,
    'fund_interest_has_company_id' as violation_type,
    'Fund interest instruments should not have company_id' as violation_message
  FROM instrument_validation
  WHERE instrument_type = 'FUND_INTEREST'
    AND company_id IS NOT NULL

  UNION ALL

  -- Rule 3: Termination date must be after inception date
  SELECT 
    id,
    instrument_type,
    'invalid_date_range' as violation_type,
    'Termination date must be after inception date' as violation_message
  FROM instrument_validation
  WHERE termination_date IS NOT NULL
    AND inception_date IS NOT NULL
    AND termination_date <= inception_date

  UNION ALL

  -- Rule 4: All instruments must have valid fund reference
  SELECT 
    id,
    instrument_type,
    'invalid_fund_reference' as violation_type,
    'Instrument has invalid fund reference' as violation_message
  FROM instrument_validation
  WHERE fund_name IS NULL

  UNION ALL

  -- Rule 5: Equity instruments must have valid company reference
  SELECT 
    id,
    instrument_type,
    'invalid_company_reference' as violation_type,
    'Equity instrument has invalid company reference' as violation_message
  FROM instrument_validation
  WHERE instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
    AND company_id IS NOT NULL
    AND company_name IS NULL
)

SELECT 
  id,
  instrument_type,
  violation_type,
  violation_message
FROM business_rule_violations