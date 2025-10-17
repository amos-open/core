-- Comprehensive DBML enum validation tests
-- Validates all enum fields match DBML specifications exactly

-- Test instrument_type enum values
SELECT 
    'invalid_instrument_type' as test_name,
    id as record_id,
    instrument_type as invalid_value
FROM {{ ref('instrument') }}
WHERE instrument_type NOT IN ('EQUITY', 'LOAN', 'CONVERTIBLE', 'WARRANT', 'FUND_INTEREST')
   OR instrument_type IS NULL

UNION ALL

-- Test transaction_type enum values
SELECT 
    'invalid_transaction_type' as test_name,
    id as record_id,
    transaction_type as invalid_value
FROM {{ ref('transaction') }}
WHERE transaction_type NOT IN (
    'DRAWDOWN', 'DISTRIBUTION', 'DIVIDEND', 'INVESTMENT_TRANSACTION', 
    'EXPENSE', 'MANAGEMENT_FEE', 'LOAN_RECEIVED', 'LOAN_DRAW', 
    'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT'
) OR transaction_type IS NULL

UNION ALL

-- Test cashflow_type enum values
SELECT 
    'invalid_cashflow_type' as test_name,
    id as record_id,
    cashflow_type as invalid_value
FROM {{ ref('instrument_cashflow') }}
WHERE cashflow_type NOT IN (
    'CONTRIBUTION', 'DISTRIBUTION', 'DIVIDEND', 'INTEREST', 
    'FEE', 'PRINCIPAL', 'DRAW', 'PREPAYMENT', 'OTHER'
) OR cashflow_type IS NULL

UNION ALL

-- Test instrument_snapshot source enum values
SELECT 
    'invalid_snapshot_source' as test_name,
    id as record_id,
    source as invalid_value
FROM {{ ref('instrument_snapshot') }}
WHERE source NOT IN ('ADMIN', 'INTERNAL')
   OR source IS NULL

UNION ALL

-- Test instrument_country role enum values
SELECT 
    'invalid_country_role' as test_name,
    instrument_id || '_' || country_code as record_id,
    role as invalid_value
FROM {{ ref('instrument_country') }}
WHERE role IS NOT NULL 
  AND role NOT IN ('DOMICILE', 'OPERATIONS', 'INVESTMENT', 'REGULATORY')

UNION ALL

-- Test currency code format (3-letter uppercase)
SELECT 
    'invalid_currency_format' as test_name,
    code as record_id,
    code as invalid_value
FROM {{ ref('currency') }}
WHERE LENGTH(code) != 3 
   OR code != UPPER(code)
   OR code ~ '[^A-Z]'

UNION ALL

-- Test country code format (2-letter uppercase)
SELECT 
    'invalid_country_format' as test_name,
    code as record_id,
    code as invalid_value
FROM {{ ref('country') }}
WHERE LENGTH(code) != 2 
   OR code != UPPER(code)
   OR code ~ '[^A-Z]'