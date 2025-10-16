-- Data Quality Test: Loan Cashflow Consistency
-- Tests loan cashflow calculations and balance consistency

{{ config(
    severity='error',
    tags=['data_quality', 'loan_cashflow', 'critical']
) }}

-- Test loan cashflow calculations and balance consistency
WITH cashflow_issues AS (
    SELECT 
        lc.id as cashflow_id,
        lc.loan_id,
        f.name as fund_name,
        l.loan_type,
        lc.cashflow_date,
        lc.loan_cashflow_type,
        lc.principal_amount,
        lc.interest_amount,
        lc.fee_amount,
        lc.total_amount,
        lc.outstanding_balance_before,
        lc.outstanding_balance_after,
        lc.interest_rate,
        lc.days_in_period,
        -- Calculate expected total amount
        COALESCE(lc.principal_amount, 0) + COALESCE(lc.interest_amount, 0) + COALESCE(lc.fee_amount, 0) as calculated_total,
        -- Calculate expected balance after
        CASE 
            WHEN lc.loan_cashflow_type = 'DRAW' THEN lc.outstanding_balance_before + COALESCE(lc.principal_amount, 0)
            WHEN lc.loan_cashflow_type IN ('PRINCIPAL_PAYMENT', 'PREPAYMENT') THEN lc.outstanding_balance_before - COALESCE(lc.principal_amount, 0)
            ELSE lc.outstanding_balance_before
        END as calculated_balance_after,
        -- Identify issues
        CASE 
            WHEN ABS(lc.total_amount - (COALESCE(lc.principal_amount, 0) + COALESCE(lc.interest_amount, 0) + COALESCE(lc.fee_amount, 0))) > 0.01 
                THEN 'Total amount calculation mismatch'
            WHEN lc.outstanding_balance_before < 0 
                THEN 'Negative outstanding balance before'
            WHEN lc.outstanding_balance_after < 0 
                THEN 'Negative outstanding balance after'
            WHEN lc.loan_cashflow_type = 'DRAW' AND lc.principal_amount <= 0 
                THEN 'Draw must have positive principal amount'
            WHEN lc.loan_cashflow_type IN ('PRINCIPAL_PAYMENT', 'PREPAYMENT') AND lc.principal_amount <= 0 
                THEN 'Payment must have positive principal amount'
            WHEN lc.loan_cashflow_type = 'INTEREST_PAYMENT' AND lc.interest_amount <= 0 
                THEN 'Interest payment must have positive interest amount'
            WHEN lc.loan_cashflow_type = 'FEE_PAYMENT' AND lc.fee_amount <= 0 
                THEN 'Fee payment must have positive fee amount'
            WHEN lc.cashflow_date > CURRENT_DATE() 
                THEN 'Future cashflow date'
            WHEN lc.days_in_period <= 0 OR lc.days_in_period > 366 
                THEN 'Invalid days in period'
            WHEN lc.interest_rate < 0 OR lc.interest_rate > 1 
                THEN 'Interest rate out of reasonable range'
            WHEN lc.loan_cashflow_type = 'DRAW' AND ABS(lc.outstanding_balance_after - (lc.outstanding_balance_before + COALESCE(lc.principal_amount, 0))) > 0.01 
                THEN 'Balance after draw calculation mismatch'
            WHEN lc.loan_cashflow_type IN ('PRINCIPAL_PAYMENT', 'PREPAYMENT') AND ABS(lc.outstanding_balance_after - (lc.outstanding_balance_before - COALESCE(lc.principal_amount, 0))) > 0.01 
                THEN 'Balance after payment calculation mismatch'
            ELSE NULL
        END as issue_description
    FROM {{ ref('loan_cashflow') }} lc
    JOIN {{ ref('loan') }} l ON lc.loan_id = l.id
    JOIN {{ ref('facility') }} fac ON l.facility_id = fac.id
    JOIN {{ ref('fund') }} f ON fac.fund_id = f.id
    WHERE lc.cashflow_date >= CURRENT_DATE() - INTERVAL '90 days'  -- Focus on recent data
)

SELECT 
    cashflow_id,
    loan_id,
    fund_name,
    loan_type,
    cashflow_date,
    loan_cashflow_type,
    issue_description,
    principal_amount,
    interest_amount,
    fee_amount,
    total_amount,
    calculated_total,
    outstanding_balance_before,
    outstanding_balance_after,
    calculated_balance_after,
    interest_rate,
    days_in_period
FROM cashflow_issues
WHERE issue_description IS NOT NULL
ORDER BY cashflow_date DESC, fund_name