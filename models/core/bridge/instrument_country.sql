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
    CAST(processed_at AS DATE) as valid_from,
    CAST(null AS DATE) as valid_to,
    normalized_allocation_percentage as allocation_pct,
    'INVESTMENT' as role,
    is_primary_geography as primary_flag,
    CAST(processed_at AS TIMESTAMP_NTZ) as created_at,
    CAST(processed_at AS TIMESTAMP_NTZ) as updated_at
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