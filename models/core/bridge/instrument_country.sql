{{
  config(
    materialized='table',
    cluster_by=['instrument_id'],
    tags=['core', 'bridge']
  )
}}

WITH staging_instrument_country AS (
  SELECT * FROM {{ ref('int_relationships_company_geography') }}
),

validated_instrument_country AS (
  SELECT
    null as instrument_id,
    country_code,
    processed_at as valid_from,
    null as valid_to,
    normalized_allocation_percentage as allocation_pct,
    geography_type as role,
    is_primary_geography as primary_flag,
    processed_at as created_at,
    processed_at as updated_at
  FROM staging_instrument_country
  WHERE 1=1
    -- Basic validation
    AND instrument_id IS NOT NULL
    AND country_code IS NOT NULL
    -- Business rule validation
    AND primary_flag IN (TRUE, FALSE)
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    AND LENGTH(country_code) = 2
    -- Temporal validation
    AND (valid_to IS NULL OR valid_from <= valid_to)
)

SELECT
  instrument_id,
  country_code,
  valid_from,
  valid_to,
  allocation_pct,
  role,
  primary_flag,
  created_at,
  updated_at
FROM validated_instrument_country