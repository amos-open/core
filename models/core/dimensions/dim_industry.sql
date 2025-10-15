-- Industry dimension table with classification hierarchy
-- Transforms staging industry data to canonical format with GICS validation

{{ config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
) }}

WITH staging_industry AS (
  SELECT * FROM {{ ref('stg_industry') }}
),

transformed AS (
  SELECT 
    UPPER(TRIM(industry_code)) AS code,
    TRIM(industry_name) AS name,
    TRIM(sector) AS sector,
    TRIM(subsector) AS subsector,
    TRIM(industry_group) AS industry_group,
    COALESCE(is_active, TRUE) AS is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at
  FROM staging_industry
  WHERE industry_code IS NOT NULL
    AND TRIM(industry_code) != ''
    AND industry_name IS NOT NULL
    AND TRIM(industry_name) != ''
    AND sector IS NOT NULL
    AND TRIM(sector) != ''
)

SELECT 
  code,
  name,
  sector,
  subsector,
  industry_group,
  is_active,
  created_at,
  updated_at
FROM transformed