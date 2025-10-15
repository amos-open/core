{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_industry AS (
  SELECT * FROM {{ ref('stg_industry') }}
),

validated_industry AS (
  SELECT
    id,
    name,
    created_at,
    updated_at
  FROM staging_industry
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND name IS NOT NULL
)

SELECT
  id,
  name,
  created_at,
  updated_at
FROM validated_industry