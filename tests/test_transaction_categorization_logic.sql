-- Test that transaction categorization is consistent with related entity references
-- Investment/divestment transactions should have instrument_id
-- Capital calls/distributions should have commitment_id
-- Note: Loan-specific transactions now use instrument_id instead of loan_id
SELECT 
    id,
    tx_type,
    instrument_id,
    facility_id,
    commitment_id,
    CASE 
        WHEN tx_type IN ('INVESTMENT_TRANSACTION', 'DRAWDOWN', 'DISTRIBUTION', 'DIVIDEND') AND instrument_id IS NULL THEN 'Missing instrument_id for instrument transaction'
        WHEN tx_type IN ('LOAN_RECEIVED', 'LOAN_DRAW', 'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT') AND (instrument_id IS NULL AND facility_id IS NULL) THEN 'Missing instrument_id or facility_id for loan transaction'
        WHEN tx_type IN ('DRAWDOWN', 'DISTRIBUTION') AND commitment_id IS NULL THEN 'Missing commitment_id for capital transaction'
        ELSE NULL
    END as validation_error
FROM {{ ref('transaction') }}
WHERE CASE 
    WHEN tx_type IN ('INVESTMENT_TRANSACTION', 'DRAWDOWN', 'DISTRIBUTION', 'DIVIDEND') AND instrument_id IS NULL THEN TRUE
    WHEN tx_type IN ('LOAN_RECEIVED', 'LOAN_DRAW', 'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT') AND (instrument_id IS NULL AND facility_id IS NULL) THEN TRUE
    WHEN tx_type IN ('DRAWDOWN', 'DISTRIBUTION') AND commitment_id IS NULL THEN TRUE
    ELSE FALSE
END