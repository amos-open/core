-- Test that transaction categorization is consistent with related entity references
-- Investment/divestment transactions should have investment_id
-- Loan transactions should have loan_id
-- Capital calls/distributions should have commitment_id
SELECT 
    id,
    tx_type,
    investment_id,
    loan_id,
    commitment_id,
    CASE 
        WHEN tx_type IN ('INVESTMENT', 'DIVESTMENT') AND investment_id IS NULL THEN 'Missing investment_id for investment transaction'
        WHEN tx_type IN ('LOAN_DRAW', 'LOAN_REPAYMENT', 'INTEREST_PAYMENT') AND loan_id IS NULL THEN 'Missing loan_id for loan transaction'
        WHEN tx_type IN ('CAPITAL_CALL', 'DISTRIBUTION') AND commitment_id IS NULL THEN 'Missing commitment_id for capital transaction'
        ELSE NULL
    END as validation_error
FROM {{ ref('transaction') }}
WHERE CASE 
    WHEN tx_type IN ('INVESTMENT', 'DIVESTMENT') AND investment_id IS NULL THEN TRUE
    WHEN tx_type IN ('LOAN_DRAW', 'LOAN_REPAYMENT', 'INTEREST_PAYMENT') AND loan_id IS NULL THEN TRUE
    WHEN tx_type IN ('CAPITAL_CALL', 'DISTRIBUTION') AND commitment_id IS NULL THEN TRUE
    ELSE FALSE
END