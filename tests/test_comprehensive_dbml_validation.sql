-- Comprehensive DBML Specification Validation Test
-- Task 5.1: Run comprehensive validation against DBML specification
-- Requirements: 6.1, 6.2, 6.3, 6.4, 6.5

-- This test validates that all models match DBML table definitions exactly
-- including column names, data types, constraints, relationships, and accepted values

WITH validation_results AS (
  
  -- 1. SCHEMA VALIDATION: Validate all model schemas match DBML table definitions exactly
  
  -- Test instrument table schema compliance
  SELECT 
    'instrument_schema_validation' as validation_category,
    'missing_required_columns' as test_name,
    'instrument' as table_name,
    'Schema validation failed - missing required columns' as error_message
  FROM (
    SELECT 1 as dummy
  ) dummy
  WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = UPPER('{{ ref("instrument").name }}')
      AND column_name IN ('ID', 'FUND_ID', 'COMPANY_ID', 'INSTRUMENT_TYPE', 'BASE_CURRENCY_CODE', 'INCEPTION_DATE', 'TERMINATION_DATE', 'DESCRIPTION', 'CREATED_AT', 'UPDATED_AT')
  )
  
  UNION ALL
  
  -- Test instrument_snapshot table schema compliance
  SELECT 
    'instrument_snapshot_schema_validation' as validation_category,
    'missing_required_columns' as test_name,
    'instrument_snapshot' as table_name,
    'Schema validation failed - missing required columns' as error_message
  FROM (
    SELECT 1 as dummy
  ) dummy
  WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = UPPER('{{ ref("instrument_snapshot").name }}')
      AND column_name IN ('ID', 'INSTRUMENT_ID', 'AS_OF_DATE', 'CURRENCY_CODE', 'FX_RATE', 'FAIR_VALUE', 'AMORTIZED_COST', 'FAIR_VALUE_CONVERTED', 'AMORTIZED_COST_CONVERTED', 'PRINCIPAL_OUTSTANDING', 'UNDRAWN_COMMITMENT', 'ACCRUED_INCOME', 'ACCRUED_FEES', 'PRINCIPAL_OUTSTANDING_CONVERTED', 'UNDRAWN_COMMITMENT_CONVERTED', 'ACCRUED_INCOME_CONVERTED', 'ACCRUED_FEES_CONVERTED', 'EQUITY_STAKE_PCT', 'EQUITY_DIVIDENDS_CUM', 'EQUITY_EXIT_PROCEEDS_ACTUAL', 'EQUITY_EXIT_PROCEEDS_FORECAST', 'SOURCE', 'SOURCE_FILE_REF', 'CREATED_AT', 'UPDATED_AT')
  )
  
  UNION ALL
  
  -- Test transaction table schema compliance
  SELECT 
    'transaction_schema_validation' as validation_category,
    'missing_required_columns' as test_name,
    'transaction' as table_name,
    'Schema validation failed - missing required columns' as error_message
  FROM (
    SELECT 1 as dummy
  ) dummy
  WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = UPPER('{{ ref("transaction").name }}')
      AND column_name IN ('ID', 'FUND_ID', 'INSTRUMENT_ID', 'FACILITY_ID', 'COMMITMENT_ID', 'TRANSACTION_TYPE', 'TRANSACTION_DATE', 'AMOUNT', 'CURRENCY_CODE', 'DESCRIPTION', 'REFERENCE_NUMBER', 'COUNTERPARTY_ID', 'CREATED_AT', 'UPDATED_AT')
  )
  
  UNION ALL
  
  -- Test instrument_cashflow table schema compliance
  SELECT 
    'instrument_cashflow_schema_validation' as validation_category,
    'missing_required_columns' as test_name,
    'instrument_cashflow' as table_name,
    'Schema validation failed - missing required columns' as error_message
  FROM (
    SELECT 1 as dummy
  ) dummy
  WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = UPPER('{{ ref("instrument_cashflow").name }}')
      AND column_name IN ('ID', 'INSTRUMENT_ID', 'TRANSACTION_ID', 'CASHFLOW_TYPE', 'CASHFLOW_DATE', 'AMOUNT', 'CURRENCY_CODE', 'DESCRIPTION', 'CREATED_AT', 'UPDATED_AT')
  )
  
  UNION ALL
  
  -- 2. ACCEPTED VALUES VALIDATION: Test all accepted values validations against DBML definitions
  
  -- Validate instrument_type enum values
  SELECT 
    'enum_validation' as validation_category,
    'invalid_instrument_type' as test_name,
    i.id as table_name,
    'Invalid instrument_type: ' || COALESCE(i.instrument_type, 'NULL') as error_message
  FROM {{ ref('instrument') }} i
  WHERE i.instrument_type NOT IN ('EQUITY', 'LOAN', 'CONVERTIBLE', 'WARRANT', 'FUND_INTEREST')
     OR i.instrument_type IS NULL
  
  UNION ALL
  
  -- Validate transaction_type enum values
  SELECT 
    'enum_validation' as validation_category,
    'invalid_transaction_type' as test_name,
    t.id as table_name,
    'Invalid transaction_type: ' || COALESCE(t.transaction_type, 'NULL') as error_message
  FROM {{ ref('transaction') }} t
  WHERE t.transaction_type NOT IN (
      'DRAWDOWN', 'DISTRIBUTION', 'DIVIDEND', 'INVESTMENT_TRANSACTION', 
      'EXPENSE', 'MANAGEMENT_FEE', 'LOAN_RECEIVED', 'LOAN_DRAW', 
      'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT'
  ) OR t.transaction_type IS NULL
  
  UNION ALL
  
  -- Validate cashflow_type enum values
  SELECT 
    'enum_validation' as validation_category,
    'invalid_cashflow_type' as test_name,
    ic.id as table_name,
    'Invalid cashflow_type: ' || COALESCE(ic.cashflow_type, 'NULL') as error_message
  FROM {{ ref('instrument_cashflow') }} ic
  WHERE ic.cashflow_type NOT IN (
      'CONTRIBUTION', 'DISTRIBUTION', 'DIVIDEND', 'INTEREST', 
      'FEE', 'PRINCIPAL', 'DRAW', 'PREPAYMENT', 'OTHER'
  ) OR ic.cashflow_type IS NULL
  
  UNION ALL
  
  -- Validate instrument_snapshot source enum values
  SELECT 
    'enum_validation' as validation_category,
    'invalid_snapshot_source' as test_name,
    s.id as table_name,
    'Invalid source: ' || COALESCE(s.source, 'NULL') as error_message
  FROM {{ ref('instrument_snapshot') }} s
  WHERE s.source NOT IN ('ADMIN', 'INTERNAL')
     OR s.source IS NULL
  
  UNION ALL
  
  -- 3. RELATIONSHIPS AND CONSTRAINTS: Verify all relationships and constraints match DBML specifications
  
  -- Validate instrument foreign key relationships
  SELECT 
    'relationship_validation' as validation_category,
    'instrument_invalid_fund_reference' as test_name,
    i.id as table_name,
    'Invalid fund_id reference: ' || i.fund_id as error_message
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('fund') }} f ON i.fund_id = f.id
  WHERE f.id IS NULL
  
  UNION ALL
  
  SELECT 
    'relationship_validation' as validation_category,
    'instrument_invalid_company_reference' as test_name,
    i.id as table_name,
    'Invalid company_id reference: ' || i.company_id as error_message
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('company') }} c ON i.company_id = c.id
  WHERE i.company_id IS NOT NULL AND c.id IS NULL
  
  UNION ALL
  
  SELECT 
    'relationship_validation' as validation_category,
    'instrument_invalid_currency_reference' as test_name,
    i.id as table_name,
    'Invalid base_currency_code reference: ' || i.base_currency_code as error_message
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('currency') }} cur ON i.base_currency_code = cur.code
  WHERE cur.code IS NULL
  
  UNION ALL
  
  -- Validate instrument_snapshot foreign key relationships
  SELECT 
    'relationship_validation' as validation_category,
    'snapshot_invalid_instrument_reference' as test_name,
    s.id as table_name,
    'Invalid instrument_id reference: ' || s.instrument_id as error_message
  FROM {{ ref('instrument_snapshot') }} s
  LEFT JOIN {{ ref('instrument') }} i ON s.instrument_id = i.id
  WHERE i.id IS NULL
  
  UNION ALL
  
  SELECT 
    'relationship_validation' as validation_category,
    'snapshot_invalid_currency_reference' as test_name,
    s.id as table_name,
    'Invalid currency_code reference: ' || s.currency_code as error_message
  FROM {{ ref('instrument_snapshot') }} s
  LEFT JOIN {{ ref('currency') }} c ON s.currency_code = c.code
  WHERE c.code IS NULL
  
  UNION ALL
  
  -- Validate transaction foreign key relationships
  SELECT 
    'relationship_validation' as validation_category,
    'transaction_invalid_fund_reference' as test_name,
    t.id as table_name,
    'Invalid fund_id reference: ' || t.fund_id as error_message
  FROM {{ ref('transaction') }} t
  LEFT JOIN {{ ref('fund') }} f ON t.fund_id = f.id
  WHERE f.id IS NULL
  
  UNION ALL
  
  SELECT 
    'relationship_validation' as validation_category,
    'transaction_invalid_instrument_reference' as test_name,
    t.id as table_name,
    'Invalid instrument_id reference: ' || t.instrument_id as error_message
  FROM {{ ref('transaction') }} t
  LEFT JOIN {{ ref('instrument') }} i ON t.instrument_id = i.id
  WHERE t.instrument_id IS NOT NULL AND i.id IS NULL
  
  UNION ALL
  
  -- Validate instrument_cashflow foreign key relationships
  SELECT 
    'relationship_validation' as validation_category,
    'cashflow_invalid_instrument_reference' as test_name,
    ic.id as table_name,
    'Invalid instrument_id reference: ' || ic.instrument_id as error_message
  FROM {{ ref('instrument_cashflow') }} ic
  LEFT JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
  WHERE i.id IS NULL
  
  UNION ALL
  
  SELECT 
    'relationship_validation' as validation_category,
    'cashflow_invalid_transaction_reference' as test_name,
    ic.id as table_name,
    'Invalid transaction_id reference: ' || ic.transaction_id as error_message
  FROM {{ ref('instrument_cashflow') }} ic
  LEFT JOIN {{ ref('transaction') }} t ON ic.transaction_id = t.id
  WHERE ic.transaction_id IS NOT NULL AND t.id IS NULL
  
  UNION ALL
  
  -- 4. BUSINESS RULE VALIDATION: Validate DBML business rules and constraints
  
  -- Validate equity instruments have company_id (DBML business rule)
  SELECT 
    'business_rule_validation' as validation_category,
    'equity_missing_company_id' as test_name,
    i.id as table_name,
    'Equity instrument missing company_id: ' || i.instrument_type as error_message
  FROM {{ ref('instrument') }} i
  WHERE i.instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
    AND i.company_id IS NULL
  
  UNION ALL
  
  -- Validate percentage constraints
  SELECT 
    'business_rule_validation' as validation_category,
    'invalid_equity_stake_percentage' as test_name,
    s.id as table_name,
    'Invalid equity_stake_pct: ' || s.equity_stake_pct::varchar as error_message
  FROM {{ ref('instrument_snapshot') }} s
  WHERE s.equity_stake_pct IS NOT NULL 
    AND (s.equity_stake_pct < 0 OR s.equity_stake_pct > 100)
  
  UNION ALL
  
  -- Validate date constraints
  SELECT 
    'business_rule_validation' as validation_category,
    'future_snapshot_date' as test_name,
    s.id as table_name,
    'Future as_of_date: ' || s.as_of_date::varchar as error_message
  FROM {{ ref('instrument_snapshot') }} s
  WHERE s.as_of_date > CURRENT_DATE()
  
  UNION ALL
  
  -- Validate monetary amount constraints (non-negative)
  SELECT 
    'business_rule_validation' as validation_category,
    'negative_fair_value' as test_name,
    s.id as table_name,
    'Negative fair_value: ' || s.fair_value::varchar as error_message
  FROM {{ ref('instrument_snapshot') }} s
  WHERE s.fair_value IS NOT NULL AND s.fair_value < 0
  
  UNION ALL
  
  -- Validate fx_rate constraints (positive when provided)
  SELECT 
    'business_rule_validation' as validation_category,
    'invalid_fx_rate' as test_name,
    s.id as table_name,
    'Invalid fx_rate: ' || s.fx_rate::varchar as error_message
  FROM {{ ref('instrument_snapshot') }} s
  WHERE s.fx_rate IS NOT NULL AND s.fx_rate <= 0
  
  UNION ALL
  
  -- 5. DATA TYPE VALIDATION: Validate data types match DBML specifications
  
  -- Validate UUID format for instrument IDs
  SELECT 
    'data_type_validation' as validation_category,
    'invalid_uuid_format' as test_name,
    i.id as table_name,
    'Invalid UUID format: ' || i.id as error_message
  FROM {{ ref('instrument') }} i
  WHERE i.id IS NOT NULL 
    AND NOT (i.id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  
  UNION ALL
  
  -- Validate currency code format (3-letter uppercase)
  SELECT 
    'data_type_validation' as validation_category,
    'invalid_currency_format' as test_name,
    c.code as table_name,
    'Invalid currency code format: ' || c.code as error_message
  FROM {{ ref('currency') }} c
  WHERE LENGTH(c.code) != 3 
     OR c.code != UPPER(c.code)
     OR c.code ~ '[^A-Z]'
  
  UNION ALL
  
  -- Validate varchar length constraints
  SELECT 
    'data_type_validation' as validation_category,
    'instrument_type_too_long' as test_name,
    i.id as table_name,
    'instrument_type exceeds 20 chars: ' || i.instrument_type as error_message
  FROM {{ ref('instrument') }} i
  WHERE LENGTH(i.instrument_type) > 20
  
  UNION ALL
  
  SELECT 
    'data_type_validation' as validation_category,
    'transaction_type_too_long' as test_name,
    t.id as table_name,
    'transaction_type exceeds 30 chars: ' || t.transaction_type as error_message
  FROM {{ ref('transaction') }} t
  WHERE LENGTH(t.transaction_type) > 30
  
  UNION ALL
  
  SELECT 
    'data_type_validation' as validation_category,
    'cashflow_type_too_long' as test_name,
    ic.id as table_name,
    'cashflow_type exceeds 20 chars: ' || ic.cashflow_type as error_message
  FROM {{ ref('instrument_cashflow') }} ic
  WHERE LENGTH(ic.cashflow_type) > 20
)

-- Return all validation failures
SELECT 
  validation_category,
  test_name,
  table_name as record_identifier,
  error_message,
  CURRENT_TIMESTAMP() as validation_timestamp
FROM validation_results
ORDER BY validation_category, test_name, table_name