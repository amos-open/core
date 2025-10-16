{{
  config(
    materialized='table',
    cluster_by=['facility_id', 'industry_id', 'valid_from'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_facility_industry AS (
  SELECT * FROM {{ ref('stg_facility_industry') }}
),

validated_facility_industry AS (
  SELECT
    facility_id,
    industry_id,
    allocation_pct,
    valid_from,
    valid_to,
    created_at,
    updated_at
  FROM staging_facility_industry
  WHERE 1=1
    -- Basic validation
    AND facility_id IS NOT NULL
    AND industry_id IS NOT NULL
    AND valid_from IS NOT NULL
    -- Business rule validation
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    -- Temporal validation
    AND (valid_to IS NULL OR valid_to > valid_from)
)

SELECT
  facility_id,
  industry_id,
  allocation_pct,
  valid_from,
  valid_to,
  created_at,
  updated_at
FROM validated_facility_industry