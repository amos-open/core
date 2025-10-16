{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_investor_type AS (
  SELECT * FROM {{ ref('stg_investor_type') }}
),

validated_investor_type AS (
  SELECT
    id,
    name,
    kyc_category,
    created_at,
    updated_at
  FROM staging_investor_type
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
)

SELECT
  id,
  name,
  kyc_category,
  created_at,
  updated_at
FROM validated_investor_type