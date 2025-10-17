-- Test to ensure instrument cashflows are consistent with transactions and instruments
-- Validates the unified instrument cashflow approach

WITH instrument_cashflow_validation AS (
  SELECT 
    ic.id as cashflow_id,
    ic.instrument_id,
    ic.transaction_id,
    ic.cashflow_type,
    ic.amount,
    ic.currency_code,
    i.instrument_type,
    t.tx_type as transaction_type,
    t.amount as transaction_amount
  FROM {{ ref('instrument_cashflow') }} ic
  LEFT JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
  LEFT JOIN {{ ref('transaction') }} t ON ic.transaction_id = t.id
),

cashflow_violations AS (
  -- Rule 1: All instrument cashflows must have valid instrument reference
  SELECT 
    cashflow_id,
    instrument_id,
    'invalid_instrument_reference' as violation_type,
    'Instrument cashflow has invalid instrument reference' as violation_message
  FROM instrument_cashflow_validation
  WHERE instrument_type IS NULL

  UNION ALL

  -- Rule 2: All instrument cashflows with transaction_id must have valid transaction reference
  SELECT 
    cashflow_id,
    instrument_id,
    'invalid_transaction_reference' as violation_type,
    'Instrument cashflow has invalid transaction reference' as violation_message
  FROM instrument_cashflow_validation
  WHERE transaction_id IS NOT NULL
    AND transaction_type IS NULL

  UNION ALL

  -- Rule 3: Cashflow type must be valid enum value
  SELECT 
    cashflow_id,
    instrument_id,
    'invalid_cashflow_type' as violation_type,
    'Invalid cashflow_type value' as violation_message
  FROM instrument_cashflow_validation
  WHERE cashflow_type NOT IN (
    'CONTRIBUTION', 'DISTRIBUTION', 'DIVIDEND', 'INTEREST', 
    'FEE', 'PRINCIPAL', 'DRAW', 'PREPAYMENT', 'OTHER'
  )

  UNION ALL

  -- Rule 4: Cashflow amounts should align with transaction amounts when linked
  SELECT 
    cashflow_id,
    instrument_id,
    'amount_mismatch_with_transaction' as violation_type,
    'Cashflow amount does not match linked transaction amount' as violation_message
  FROM instrument_cashflow_validation
  WHERE transaction_id IS NOT NULL
    AND ABS(amount - transaction_amount) > 0.01  -- Allow for small rounding differences

  UNION ALL

  -- Rule 5: Equity-specific cashflow types should only be used with equity instruments
  SELECT 
    cashflow_id,
    instrument_id,
    'invalid_cashflow_type_for_instrument' as violation_type,
    'Equity cashflow type used with non-equity instrument' as violation_message
  FROM instrument_cashflow_validation
  WHERE cashflow_type IN ('DIVIDEND')
    AND instrument_type NOT IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')

  UNION ALL

  -- Rule 6: Loan-specific cashflow types should only be used with loan instruments
  SELECT 
    cashflow_id,
    instrument_id,
    'invalid_cashflow_type_for_instrument' as violation_type,
    'Loan cashflow type used with non-loan instrument' as violation_message
  FROM instrument_cashflow_validation
  WHERE cashflow_type IN ('INTEREST', 'PRINCIPAL', 'DRAW', 'PREPAYMENT')
    AND instrument_type != 'LOAN'
)

SELECT 
  cashflow_id,
  instrument_id,
  violation_type,
  violation_message
FROM cashflow_violations