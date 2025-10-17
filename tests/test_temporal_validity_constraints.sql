-- Test that temporal validity constraints are properly enforced across all temporal bridge tables
-- This test ensures that valid_to is always greater than valid_from when not null
-- Updated for instrument-centric approach with temporal bridge tables

WITH temporal_violations AS (
  -- Check instrument_country temporal validity
  SELECT 
    'instrument_country' as table_name,
    instrument_id as entity_id,
    country_code as related_id,
    valid_from,
    valid_to,
    'valid_to_before_valid_from' as violation_type
  FROM {{ ref('instrument_country') }}
  WHERE valid_to IS NOT NULL
    AND valid_to <= valid_from

  UNION ALL

  -- Check instrument_industry temporal validity
  SELECT 
    'instrument_industry' as table_name,
    instrument_id as entity_id,
    industry_id as related_id,
    valid_from,
    valid_to,
    'valid_to_before_valid_from' as violation_type
  FROM {{ ref('instrument_industry') }}
  WHERE valid_to IS NOT NULL
    AND valid_to <= valid_from

  UNION ALL

  -- Check for overlapping periods in instrument_country
  SELECT 
    'instrument_country' as table_name,
    ic1.instrument_id as entity_id,
    ic1.country_code as related_id,
    ic1.valid_from,
    ic1.valid_to,
    'overlapping_periods' as violation_type
  FROM {{ ref('instrument_country') }} ic1
  JOIN {{ ref('instrument_country') }} ic2 
    ON ic1.instrument_id = ic2.instrument_id 
    AND ic1.country_code = ic2.country_code
    AND ic1.valid_from != ic2.valid_from
  WHERE ic1.valid_from < COALESCE(ic2.valid_to, '9999-12-31'::date)
    AND COALESCE(ic1.valid_to, '9999-12-31'::date) > ic2.valid_from

  UNION ALL

  -- Check for overlapping periods in instrument_industry
  SELECT 
    'instrument_industry' as table_name,
    ii1.instrument_id as entity_id,
    ii1.industry_id as related_id,
    ii1.valid_from,
    ii1.valid_to,
    'overlapping_periods' as violation_type
  FROM {{ ref('instrument_industry') }} ii1
  JOIN {{ ref('instrument_industry') }} ii2 
    ON ii1.instrument_id = ii2.instrument_id 
    AND ii1.industry_id = ii2.industry_id
    AND ii1.valid_from != ii2.valid_from
  WHERE ii1.valid_from < COALESCE(ii2.valid_to, '9999-12-31'::date)
    AND COALESCE(ii1.valid_to, '9999-12-31'::date) > ii2.valid_from
)

SELECT 
  table_name,
  entity_id,
  related_id,
  valid_from,
  valid_to,
  violation_type
FROM temporal_violations