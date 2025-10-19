{{
  config(
    materialized='table',
    cluster_by=['company_id', 'country_code'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_company_country AS (
  SELECT * FROM {{ ref('int_relationships_company_geography') }}
),

validated_company_country AS (
  SELECT
    canonical_company_id as company_id,
    country_code,
    is_primary_geography as primary_flag,
    normalized_allocation_percentage as allocation_pct,
    processed_at as created_at,
    processed_at as updated_at
  FROM staging_company_country
  WHERE 1=1
    -- Basic validation
    AND company_id IS NOT NULL
    AND country_code IS NOT NULL
    -- Business rule validation
    AND primary_flag IN (TRUE, FALSE)
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    AND LENGTH(country_code) = 2
)

SELECT
  company_id,
  country_code,
  primary_flag,
  allocation_pct,
  created_at,
  updated_at
FROM validated_company_country