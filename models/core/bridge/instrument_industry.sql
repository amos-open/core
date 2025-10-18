{{
  config(
    materialized='table',
    cluster_by=['instrument_id'],
    tags=['core', 'bridge']
  )
}}

WITH staging_instrument_industry AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_relationships_company_industry') }}
),

validated_instrument_industry AS (
  SELECT
    instrument_id,
    industry_id,
    allocation_pct,
    primary_flag,
    valid_from,
    valid_to,
    created_at,
    updated_at
  FROM staging_instrument_industry
  WHERE 1=1
    -- Basic validation
    AND instrument_id IS NOT NULL
    AND industry_id IS NOT NULL
    -- Business rule validation
    AND primary_flag IN (TRUE, FALSE)
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    -- Temporal validation
    AND (valid_to IS NULL OR valid_from <= valid_to)
)

SELECT
  instrument_id,
  industry_id,
  allocation_pct,
  primary_flag,
  valid_from,
  valid_to,
  created_at,
  updated_at
FROM validated_instrument_industry