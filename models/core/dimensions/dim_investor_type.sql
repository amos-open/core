-- Investor type dimension table for KYC compliance and categorization
-- Transforms staging investor type data to canonical format with KYC validation

{{ config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
) }}

WITH staging_investor_type AS (
  SELECT * FROM {{ ref('stg_investor_type') }}
),

transformed AS (
  SELECT 
    UPPER(TRIM(investor_type_code)) AS code,
    TRIM(investor_type_name) AS name,
    TRIM(description) AS description,
    UPPER(TRIM(category)) AS category,
    COALESCE(requires_kyc, TRUE) AS requires_kyc,
    COALESCE(is_active, TRUE) AS is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at
  FROM staging_investor_type
  WHERE investor_type_code IS NOT NULL
    AND TRIM(investor_type_code) != ''
    AND investor_type_name IS NOT NULL
    AND TRIM(investor_type_name) != ''
    AND category IS NOT NULL
    AND TRIM(category) != ''
)

SELECT 
  code,
  name,
  description,
  category,
  requires_kyc,
  is_active,
  created_at,
  updated_at
FROM transformed