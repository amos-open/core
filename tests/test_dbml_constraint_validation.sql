-- Comprehensive DBML constraint validation tests
-- Validates all foreign key relationships and business rules

-- Test foreign key constraints for instrument table
SELECT 
    'instrument_invalid_fund_id' as test_name,
    i.id as record_id,
    i.fund_id as invalid_reference
FROM {{ ref('instrument') }} i
LEFT JOIN {{ ref('fund') }} f ON i.fund_id = f.id
WHERE f.id IS NULL

UNION ALL

SELECT 
    'instrument_invalid_company_id' as test_name,
    i.id as record_id,
    i.company_id as invalid_reference
FROM {{ ref('instrument') }} i
LEFT JOIN {{ ref('company') }} c ON i.company_id = c.id
WHERE i.company_id IS NOT NULL AND c.id IS NULL

UNION ALL

SELECT 
    'instrument_invalid_currency_code' as test_name,
    i.id as record_id,
    i.base_currency_code as invalid_reference
FROM {{ ref('instrument') }} i
LEFT JOIN {{ ref('currency') }} cur ON i.base_currency_code = cur.code
WHERE cur.code IS NULL

UNION ALL

-- Test foreign key constraints for instrument_snapshot table
SELECT 
    'snapshot_invalid_instrument_id' as test_name,
    s.id as record_id,
    s.instrument_id as invalid_reference
FROM {{ ref('instrument_snapshot') }} s
LEFT JOIN {{ ref('instrument') }} i ON s.instrument_id = i.id
WHERE i.id IS NULL

UNION ALL

SELECT 
    'snapshot_invalid_currency_code' as test_name,
    s.id as record_id,
    s.currency_code as invalid_reference
FROM {{ ref('instrument_snapshot') }} s
LEFT JOIN {{ ref('currency') }} c ON s.currency_code = c.code
WHERE c.code IS NULL

UNION ALL

-- Test foreign key constraints for transaction table
SELECT 
    'transaction_invalid_fund_id' as test_name,
    t.id as record_id,
    t.fund_id as invalid_reference
FROM {{ ref('transaction') }} t
LEFT JOIN {{ ref('fund') }} f ON t.fund_id = f.id
WHERE f.id IS NULL

UNION ALL

SELECT 
    'transaction_invalid_instrument_id' as test_name,
    t.id as record_id,
    t.instrument_id as invalid_reference
FROM {{ ref('transaction') }} t
LEFT JOIN {{ ref('instrument') }} i ON t.instrument_id = i.id
WHERE t.instrument_id IS NOT NULL AND i.id IS NULL

UNION ALL

-- Test foreign key constraints for instrument_cashflow table
SELECT 
    'cashflow_invalid_instrument_id' as test_name,
    ic.id as record_id,
    ic.instrument_id as invalid_reference
FROM {{ ref('instrument_cashflow') }} ic
LEFT JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
WHERE i.id IS NULL

UNION ALL

SELECT 
    'cashflow_invalid_transaction_id' as test_name,
    ic.id as record_id,
    ic.transaction_id as invalid_reference
FROM {{ ref('instrument_cashflow') }} ic
LEFT JOIN {{ ref('transaction') }} t ON ic.transaction_id = t.id
WHERE ic.transaction_id IS NOT NULL AND t.id IS NULL

UNION ALL

-- Test date constraints
SELECT 
    'snapshot_future_date' as test_name,
    s.id as record_id,
    s.as_of_date::varchar as invalid_reference
FROM {{ ref('instrument_snapshot') }} s
WHERE s.as_of_date > CURRENT_DATE()

UNION ALL

-- Test percentage constraints
SELECT 
    'invalid_allocation_percentage' as test_name,
    ic.instrument_id || '_' || ic.country_code as record_id,
    ic.allocation_pct::varchar as invalid_reference
FROM {{ ref('instrument_country') }} ic
WHERE ic.allocation_pct < 0 OR ic.allocation_pct > 100

UNION ALL

SELECT 
    'invalid_equity_stake_percentage' as test_name,
    s.id as record_id,
    s.equity_stake_pct::varchar as invalid_reference
FROM {{ ref('instrument_snapshot') }} s
WHERE s.equity_stake_pct IS NOT NULL 
  AND (s.equity_stake_pct < 0 OR s.equity_stake_pct > 100)