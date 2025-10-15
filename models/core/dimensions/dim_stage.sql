-- Investment stage dimension table for investment classification
-- Transforms staging investment stage data to canonical format with order validation

{{ config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
) }}

WITH staging_stage AS (
  SELECT * FROM {{ ref('stg_investment_stage') }}
),

transformed AS (
  SELECT 
    UPPER(TRIM(stage_code)) AS code,
    TRIM(stage_name) AS name,
    TRIM(description) AS description,
    stage_order,
    UPPER(TRIM(stage_category)) AS stage_category,
    COALESCE(is_active, TRUE) AS is_active,
    COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at
  FROM staging_stage
  WHERE stage_code IS NOT NULL
    AND TRIM(stage_code) != ''
    AND stage_name IS NOT NULL
    AND TRIM(stage_name) != ''
    AND stage_order IS NOT NULL
    AND stage_order > 0
    AND stage_category IS NOT NULL
    AND TRIM(stage_category) != ''
)

SELECT 
  code,
  name,
  description,
  stage_order,
  stage_category,
  is_active,
  created_at,
  updated_at
FROM transformed