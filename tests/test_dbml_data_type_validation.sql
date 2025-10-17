-- Comprehensive DBML data type validation tests
-- Validates all fields match DBML data type specifications

-- Test UUID format validation for instrument IDs
SELECT 
    'invalid_instrument_uuid_format' as test_name,
    id as record_id,
    id as invalid_value
FROM {{ ref('instrument') }}
WHERE id IS NOT NULL 
  AND NOT (id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')

UNION ALL

-- Test decimal precision for allocation percentages
SELECT 
    'invalid_allocation_precision' as test_name,
    instrument_id || '_' || country_code as record_id,
    allocation_pct::varchar as invalid_value
FROM {{ ref('instrument_country') }}
WHERE allocation_pct IS NOT NULL
  AND (allocation_pct::varchar ~ '\.[0-9]{3,}')  -- More than 2 decimal places

UNION ALL

-- Test decimal precision for equity stake percentages
SELECT 
    'invalid_equity_stake_precision' as test_name,
    id as record_id,
    equity_stake_pct::varchar as invalid_value
FROM {{ ref('instrument_snapshot') }}
WHERE equity_stake_pct IS NOT NULL
  AND (equity_stake_pct::varchar ~ '\.[0-9]{3,}')  -- More than 2 decimal places

UNION ALL

-- Test numeric precision for monetary amounts
SELECT 
    'invalid_amount_precision' as test_name,
    id as record_id,
    amount::varchar as invalid_value
FROM {{ ref('transaction') }}
WHERE amount IS NOT NULL
  AND (amount::varchar ~ '\.[0-9]{3,}')  -- More than 2 decimal places

UNION ALL

-- Test varchar length constraints for instrument_type
SELECT 
    'instrument_type_too_long' as test_name,
    id as record_id,
    instrument_type as invalid_value
FROM {{ ref('instrument') }}
WHERE LENGTH(instrument_type) > 20

UNION ALL

-- Test varchar length constraints for transaction_type
SELECT 
    'transaction_type_too_long' as test_name,
    id as record_id,
    transaction_type as invalid_value
FROM {{ ref('transaction') }}
WHERE LENGTH(transaction_type) > 30

UNION ALL

-- Test varchar length constraints for cashflow_type
SELECT 
    'cashflow_type_too_long' as test_name,
    id as record_id,
    cashflow_type as invalid_value
FROM {{ ref('instrument_cashflow') }}
WHERE LENGTH(cashflow_type) > 20

UNION ALL

-- Test date format validation
SELECT 
    'invalid_date_format' as test_name,
    id as record_id,
    inception_date::varchar as invalid_value
FROM {{ ref('instrument') }}
WHERE inception_date IS NOT NULL
  AND NOT (inception_date::varchar ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')

UNION ALL

-- Test boolean field validation
SELECT 
    'invalid_boolean_value' as test_name,
    instrument_id || '_' || country_code as record_id,
    primary_flag::varchar as invalid_value
FROM {{ ref('instrument_country') }}
WHERE primary_flag IS NOT NULL
  AND primary_flag NOT IN (true, false)