-- Test that transaction amounts have appropriate signs based on transaction type
-- Drawdowns and investments should typically be negative (outflows from fund perspective)
-- Distributions and divestments should typically be positive (inflows to fund)
-- This is a business logic validation test
SELECT 
    id,
    tx_type,
    amount,
    CASE 
        WHEN tx_type IN ('DRAWDOWN', 'INVESTMENT_TRANSACTION', 'LOAN_DRAW', 'MANAGEMENT_FEE', 'EXPENSE') AND amount > 0 THEN 'Unexpected positive amount for outflow transaction'
        WHEN tx_type IN ('DISTRIBUTION', 'DIVIDEND', 'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT') AND amount < 0 THEN 'Unexpected negative amount for inflow transaction'
        ELSE NULL
    END as validation_warning
FROM {{ ref('transaction') }}
WHERE CASE 
    WHEN tx_type IN ('DRAWDOWN', 'INVESTMENT_TRANSACTION', 'LOAN_DRAW', 'MANAGEMENT_FEE', 'EXPENSE') AND amount > 0 THEN TRUE
    WHEN tx_type IN ('DISTRIBUTION', 'DIVIDEND', 'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT') AND amount < 0 THEN TRUE
    ELSE FALSE
END