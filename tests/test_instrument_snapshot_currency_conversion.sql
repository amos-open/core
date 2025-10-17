-- Test to ensure currency conversion calculations are consistent
-- When fx_rate is provided, converted amounts should match original amounts * fx_rate

WITH conversion_validation AS (
  SELECT 
    id,
    instrument_id,
    as_of_date,
    currency_code,
    fx_rate,
    fair_value,
    fair_value_converted,
    amortized_cost,
    amortized_cost_converted,
    principal_outstanding,
    principal_outstanding_converted,
    undrawn_commitment,
    undrawn_commitment_converted,
    accrued_income,
    accrued_income_converted,
    accrued_fees,
    accrued_fees_converted
  FROM {{ ref('instrument_snapshot') }}
  WHERE fx_rate IS NOT NULL
    AND fx_rate > 0
),

conversion_errors AS (
  SELECT 
    id,
    'fair_value_conversion_error' as error_type
  FROM conversion_validation
  WHERE fair_value IS NOT NULL 
    AND fair_value_converted IS NOT NULL
    AND ABS(fair_value_converted - (fair_value * fx_rate)) > 0.01
  
  UNION ALL
  
  SELECT 
    id,
    'amortized_cost_conversion_error' as error_type
  FROM conversion_validation
  WHERE amortized_cost IS NOT NULL 
    AND amortized_cost_converted IS NOT NULL
    AND ABS(amortized_cost_converted - (amortized_cost * fx_rate)) > 0.01
  
  UNION ALL
  
  SELECT 
    id,
    'principal_outstanding_conversion_error' as error_type
  FROM conversion_validation
  WHERE principal_outstanding IS NOT NULL 
    AND principal_outstanding_converted IS NOT NULL
    AND ABS(principal_outstanding_converted - (principal_outstanding * fx_rate)) > 0.01
  
  UNION ALL
  
  SELECT 
    id,
    'undrawn_commitment_conversion_error' as error_type
  FROM conversion_validation
  WHERE undrawn_commitment IS NOT NULL 
    AND undrawn_commitment_converted IS NOT NULL
    AND ABS(undrawn_commitment_converted - (undrawn_commitment * fx_rate)) > 0.01
  
  UNION ALL
  
  SELECT 
    id,
    'accrued_income_conversion_error' as error_type
  FROM conversion_validation
  WHERE accrued_income IS NOT NULL 
    AND accrued_income_converted IS NOT NULL
    AND ABS(accrued_income_converted - (accrued_income * fx_rate)) > 0.01
  
  UNION ALL
  
  SELECT 
    id,
    'accrued_fees_conversion_error' as error_type
  FROM conversion_validation
  WHERE accrued_fees IS NOT NULL 
    AND accrued_fees_converted IS NOT NULL
    AND ABS(accrued_fees_converted - (accrued_fees * fx_rate)) > 0.01
)

SELECT * FROM conversion_errors