-- Test to ensure all instrument cashflows reference valid instruments
-- This validates referential integrity between instrument_cashflow and instrument tables

SELECT 
    ic.id as cashflow_id,
    ic.instrument_id,
    ic.cashflow_type,
    ic.cashflow_date
FROM {{ ref('instrument_cashflow') }} ic
LEFT JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
WHERE i.id IS NULL