-- Test that outstanding balance calculations are consistent for principal payments
-- For principal payments: outstanding_balance_after should equal outstanding_balance_before - principal_amount
SELECT 
    id,
    loan_id,
    cashflow_date,
    loan_cashflow_type,
    principal_amount,
    outstanding_balance_before,
    outstanding_balance_after,
    (outstanding_balance_before - principal_amount) as calculated_balance_after
FROM {{ ref('loan_cashflow') }}
WHERE loan_cashflow_type IN ('PRINCIPAL_PAYMENT', 'PREPAYMENT')
  AND principal_amount IS NOT NULL
  AND outstanding_balance_before IS NOT NULL
  AND outstanding_balance_after IS NOT NULL
  AND ABS(outstanding_balance_after - (outstanding_balance_before - principal_amount)) > 0.01  -- Allow for small rounding differences