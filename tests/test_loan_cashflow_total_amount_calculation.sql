-- Test that total_amount equals the sum of principal_amount + interest_amount + fee_amount
SELECT 
    id,
    loan_id,
    cashflow_date,
    total_amount,
    principal_amount,
    interest_amount,
    fee_amount,
    (COALESCE(principal_amount, 0) + COALESCE(interest_amount, 0) + COALESCE(fee_amount, 0)) as calculated_total
FROM {{ ref('loan_cashflow') }}
WHERE total_amount IS NOT NULL
  AND (principal_amount IS NOT NULL OR interest_amount IS NOT NULL OR fee_amount IS NOT NULL)
  AND ABS(total_amount - (COALESCE(principal_amount, 0) + COALESCE(interest_amount, 0) + COALESCE(fee_amount, 0))) > 0.01  -- Allow for small rounding differences