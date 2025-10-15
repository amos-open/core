-- Currency dimension table with ISO currency codes
-- Transforms staging currency data to canonical format with validation

{{ config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
) }}

WITH staging_currency AS (
  SELECT * FROM {{ ref('stg_currency') }}
),

transformed AS (
  SELECT 
    UPPER(TRIM(currency_code)) AS code,
    TRIM(currency_name) AS name,
    TRIM(currency_symbol) AS symbol,
    COALESCE(decimal_places, 2) AS decimal_places,
    COALESCE(is_active, TRUE) AS is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at
  FROM staging_currency
  WHERE currency_code IS NOT NULL
    AND TRIM(currency_code) != ''
    AND LENGTH(TRIM(currency_code)) = 3
)

SELECT 
  code,
  name,
  symbol,
  decimal_places,
  is_active,
  created_at,
  updated_at
FROM transformed