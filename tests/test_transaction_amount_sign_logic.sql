-- Test that transaction amounts have appropriate signs based on transaction type
-- Capital calls and investments should typically be negative (outflows from fund perspective)
-- Distributions and divestments should typically be positive (inflows to fund)
-- This is a business logic validation test
SELECT 
    id,
    tx_type,
    amount,
    CASE 
        WHEN tx_type IN ('CAPITAL_CALL', 'INVESTMENT', 'LOAN_DRAW', 'FEE_PAYMENT', 'EXPENSE') AND amount > 0 THEN 'Unexpected positive amount for outflow transaction'
        WHEN tx_type IN ('DISTRIBUTION', 'DIVESTMENT', 'LOAN_REPAYMENT', 'INTEREST_PAYMENT', 'INCOME') AND amount < 0 THEN 'Unexpected negative amount for inflow transaction'
        ELSE NULL
    END as validation_warning
FROM {{ ref('transaction') }}
WHERE CASE 
    WHEN tx_type IN ('CAPITAL_CALL', 'INVESTMENT', 'LOAN_DRAW', 'FEE_PAYMENT', 'EXPENSE') AND amount > 0 THEN TRUE
    WHEN tx_type IN ('DISTRIBUTION', 'DIVESTMENT', 'LOAN_REPAYMENT', 'INTEREST_PAYMENT', 'INCOME') AND amount < 0 THEN TRUE
    ELSE FALSE
END