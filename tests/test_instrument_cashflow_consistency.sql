-- Test to ensure instrument cashflows align with transactions when transaction_id is provided
-- This validates referential integrity between instrument_cashflow and transaction tables

SELECT 
    ic.id as cashflow_id,
    ic.transaction_id,
    ic.instrument_id,
    ic.cashflow_type
FROM {{ ref('instrument_cashflow') }} ic
LEFT JOIN {{ ref('transaction') }} t ON ic.transaction_id = t.id
WHERE ic.transaction_id IS NOT NULL
  AND t.id IS NULL