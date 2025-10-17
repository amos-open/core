-- Test that currency conversion is consistent for unified instrument snapshots
-- Converted amounts should equal original amounts * fx_rate
-- This validates the instrument-centric currency conversion logic
SELECT 
    instrument_id,
    as_of_date,
    fair_value,
    amortized_cost,
    fair_value_converted,
    amortized_cost_converted,
    fx_rate,
    'Currency conversion validation failed' as error_message
FROM {{ ref('instrument_snapshot') }}
WHERE fair_value IS NOT NULL 
  AND amortized_cost IS NOT NULL 
  AND fair_value_converted IS NOT NULL
  AND amortized_cost_converted IS NOT NULL
  AND fx_rate IS NOT NULL
  AND fx_rate > 0
  AND (
    ABS(fair_value_converted - (fair_value * fx_rate)) > 0.01
    OR ABS(amortized_cost_converted - (amortized_cost * fx_rate)) > 0.01
  )