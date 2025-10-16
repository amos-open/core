-- Test that interest calculations are consistent with rate, balance, and days
-- Interest should approximately equal (outstanding_balance_before * interest_rate * days_in_period / 365)
SELECT 
    id,
    loan_id,
    cashflow_date,
    interest_amount,
    outstanding_balance_before,
    interest_rate,
    days_in_period,
    CASE 
        WHEN outstanding_balance_before > 0 AND interest_rate > 0 AND days_in_period > 0 
        THEN (outstanding_balance_before * interest_rate * days_in_period / 365.0)
        ELSE NULL 
    END as calculated_interest
FROM {{ ref('loan_cashflow') }}
WHERE loan_cashflow_type = 'INTEREST_PAYMENT'
  AND interest_amount IS NOT NULL
  AND outstanding_balance_before IS NOT NULL
  AND interest_rate IS NOT NULL
  AND days_in_period IS NOT NULL
  AND outstanding_balance_before > 0
  AND interest_rate > 0
  AND days_in_period > 0
  -- Allow for reasonable variance in interest calculations (different day count conventions, etc.)
  AND ABS(interest_amount - (outstanding_balance_before * interest_rate * days_in_period / 365.0)) > (outstanding_balance_before * interest_rate * days_in_period / 365.0 * 0.05)