{{
  config(
    materialized='table',
    cluster_by=['instrument_id'],
    tags=['core', 'bridge']
  )
}}

WITH staging_instrument_country AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_relationships_company_geography') }}
),

validated_instrument_country AS (
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