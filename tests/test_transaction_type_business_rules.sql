-- Test to ensure transaction types follow business rules for required relationships
-- Validates that certain transaction types have appropriate entity references

WITH transaction_validation AS (
  SELECT 
    id,
    transaction_type,
    instrument_id,
    facility_id,
    commitment_id,
    CASE 
      -- Investment transactions should have instrument_id
      WHEN transaction_type IN ('INVESTMENT_TRANSACTION', 'DIVIDEND') 
           AND instrument_id IS NULL THEN 'Missing instrument_id for investment transaction'
      
      -- Commitment transactions should have commitment_id  
      WHEN transaction_type IN ('DRAWDOWN', 'DISTRIBUTION') 
           AND commitment_id IS NULL THEN 'Missing commitment_id for commitment transaction'
      
      -- Loan transactions should have instrument_id or facility_id
      WHEN transaction_type IN ('LOAN_RECEIVED', 'LOAN_DRAW', 'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT')
           AND instrument_id IS NULL AND facility_id IS NULL THEN 'Missing instrument_id or facility_id for loan transaction'
      
      ELSE NULL
    END as validation_error
  FROM {{ ref('transaction') }}
)

SELECT 
  id,
  transaction_type,
  validation_error
FROM transaction_validation
WHERE validation_error IS NOT NULL