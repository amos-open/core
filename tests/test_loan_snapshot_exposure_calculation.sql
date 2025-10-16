-- Test that total exposure calculation is consistent with principal and accrued interest
-- Total exposure should equal outstanding_principal + accrued_interest
SELECT 
    loan_id,
    as_of_date,
    total_exposure,
    outstanding_principal,
    accrued_interest,
    (outstanding_principal + accrued_interest) as calculated_exposure
FROM {{ ref('loan_snapshot') }}
WHERE total_exposure IS NOT NULL 
  AND outstanding_principal IS NOT NULL 
  AND accrued_interest IS NOT NULL
  AND ABS(total_exposure - (outstanding_principal + accrued_interest)) > 0.01  -- Allow for small rounding differences