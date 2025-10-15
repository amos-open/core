-- Country dimension table with geographic hierarchy
-- Transforms staging country data to canonical format with geographic validation

{{ config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
) }}

WITH staging_country AS (
  SELECT * FROM {{ ref('stg_country') }}
),

transformed AS (
  SELECT 
    UPPER(TRIM(country_code_iso2)) AS code,
    UPPER(TRIM(country_code_iso3)) AS iso3_code,
    TRIM(country_name) AS name,
    TRIM(region) AS region,
    TRIM(subregion) AS subregion,
    COALESCE(is_active, TRUE) AS is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at
  FROM staging_country
  WHERE country_code_iso2 IS NOT NULL
    AND TRIM(country_code_iso2) != ''
    AND LENGTH(TRIM(country_code_iso2)) = 2
    AND country_code_iso3 IS NOT NULL
    AND TRIM(country_code_iso3) != ''
    AND LENGTH(TRIM(country_code_iso3)) = 3
)

SELECT 
  code,
  iso3_code,
  name,
  region,
  subregion,
  is_active,
  created_at,
  updated_at
FROM transformed