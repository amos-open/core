-- Comprehensive business rule validation for instrument model
-- Tests DBML-specified constraints and business logic

-- Test 1: Equity instruments must have company_id
SELECT 
    'equity_instruments_missing_company_id' as test_name,
    id as instrument_id,
    instrument_type,
    company_id
FROM {{ ref('instrument') }}
WHERE instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
  AND company_id IS NULL

UNION ALL

-- Test 2: Loan instruments should not have company_id
SELECT 
    'loan_instruments_with_company_id' as test_name,
    id as instrument_id,
    instrument_type,
    company_id
FROM {{ ref('instrument') }}
WHERE instrument_type IN ('LOAN', 'FUND_INTEREST')
  AND company_id IS NOT NULL

UNION ALL

-- Test 3: Termination date must be after inception date
SELECT 
    'invalid_date_range' as test_name,
    id as instrument_id,
    inception_date::varchar as inception_date,
    termination_date::varchar as termination_date
FROM {{ ref('instrument') }}
WHERE termination_date IS NOT NULL
  AND inception_date IS NOT NULL
  AND termination_date <= inception_date

UNION ALL

-- Test 4: Instrument type must be valid enum value
SELECT 
    'invalid_instrument_type' as test_name,
    id as instrument_id,
    instrument_type,
    NULL as extra_field
FROM {{ ref('instrument') }}
WHERE instrument_type NOT IN ('EQUITY', 'LOAN', 'CONVERTIBLE', 'WARRANT', 'FUND_INTEREST')

UNION ALL

-- Test 5: Base currency code must be valid 3-letter ISO code
SELECT 
    'invalid_currency_format' as test_name,
    id as instrument_id,
    base_currency_code,
    NULL as extra_field
FROM {{ ref('instrument') }}
WHERE base_currency_code IS NOT NULL
  AND (LENGTH(base_currency_code) != 3 OR base_currency_code ~ '[^A-Z]')