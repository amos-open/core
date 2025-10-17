-- Test to ensure transaction-instrument relationships are consistent
-- Validates that instrument-related transactions reference valid instruments

SELECT 
    t.id as transaction_id,
    t.transaction_type,
    t.instrument_id,
    t.transaction_date
FROM {{ ref('transaction') }} t
LEFT JOIN {{ ref('instrument') }} i ON t.instrument_id = i.id
WHERE t.instrument_id IS NOT NULL
  AND i.id IS NULL