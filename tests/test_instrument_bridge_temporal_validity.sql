-- Test temporal validity constraints for instrument bridge tables
-- Ensures valid_from <= valid_to and no overlapping periods for same instrument-entity pairs

WITH instrument_country_temporal AS (
  SELECT 
    'instrument_country' as table_name,
    instrument_id,
    country_code as entity_id,
    valid_from,
    valid_to
  FROM {{ ref('instrument_country') }}
  WHERE valid_to IS NOT NULL
    AND valid_from > valid_to
),

instrument_industry_temporal AS (
  SELECT 
    'instrument_industry' as table_name,
    instrument_id,
    industry_id::varchar as entity_id,
    valid_from,
    valid_to
  FROM {{ ref('instrument_industry') }}
  WHERE valid_to IS NOT NULL
    AND valid_from > valid_to
),

-- Check for overlapping periods in instrument_country
instrument_country_overlaps AS (
  SELECT 
    'instrument_country' as table_name,
    ic1.instrument_id,
    ic1.country_code as entity_id,
    ic1.valid_from,
    ic1.valid_to,
    'OVERLAPPING_PERIODS' as violation_type
  FROM {{ ref('instrument_country') }} ic1
  JOIN {{ ref('instrument_country') }} ic2 
    ON ic1.instrument_id = ic2.instrument_id
    AND ic1.country_code = ic2.country_code
    AND ic1.valid_from != ic2.valid_from
  WHERE ic1.valid_from < COALESCE(ic2.valid_to, '9999-12-31'::date)
    AND COALESCE(ic1.valid_to, '9999-12-31'::date) > ic2.valid_from
),

-- Check for overlapping periods in instrument_industry
instrument_industry_overlaps AS (
  SELECT 
    'instrument_industry' as table_name,
    ii1.instrument_id,
    ii1.industry_id::varchar as entity_id,
    ii1.valid_from,
    ii1.valid_to,
    'OVERLAPPING_PERIODS' as violation_type
  FROM {{ ref('instrument_industry') }} ii1
  JOIN {{ ref('instrument_industry') }} ii2 
    ON ii1.instrument_id = ii2.instrument_id
    AND ii1.industry_id = ii2.industry_id
    AND ii1.valid_from != ii2.valid_from
  WHERE ii1.valid_from < COALESCE(ii2.valid_to, '9999-12-31'::date)
    AND COALESCE(ii1.valid_to, '9999-12-31'::date) > ii2.valid_from
),

all_violations AS (
  SELECT table_name, instrument_id, entity_id, valid_from, valid_to, 'INVALID_DATE_RANGE' as violation_type
  FROM instrument_country_temporal
  
  UNION ALL
  
  SELECT table_name, instrument_id, entity_id, valid_from, valid_to, 'INVALID_DATE_RANGE' as violation_type
  FROM instrument_industry_temporal
  
  UNION ALL
  
  SELECT table_name, instrument_id, entity_id, valid_from, valid_to, violation_type
  FROM instrument_country_overlaps
  
  UNION ALL
  
  SELECT table_name, instrument_id, entity_id, valid_from, valid_to, violation_type
  FROM instrument_industry_overlaps
)

SELECT *
FROM all_violations