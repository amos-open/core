{{
  config(
    materialized='table',
    cluster_by=['facility_id', 'country_code', 'valid_from'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_facility_country AS (
  SELECT * FROM {{ ref('stg_facility_country') }}
),

validated_facility_country AS (
  SELECT
    facility_id,
    country_code,
    allocation_pct,
    valid_from,
    valid_to,
    created_at,
    updated_at
  FROM staging_facility_country
  WHERE 1=1
    -- Basic validation
    AND facility_id IS NOT NULL
    AND country_code IS NOT NULL
    AND valid_from IS NOT NULL
    -- Business rule validation
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    AND LENGTH(country_code) = 2
    -- Temporal validation
    AND (valid_to IS NULL OR valid_to > valid_from)
)

SELECT
  facility_id,
  country_code,
  allocation_pct,
  valid_from,
  valid_to,
  created_at,
  updated_at
FROM validated_facility_country