{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_stage AS (
  SELECT * FROM {{ ref('stg_stage') }}
),

validated_stage AS (
  SELECT
    id,
    name,
    type,
    created_at,
    updated_at
  FROM staging_stage
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
)

SELECT
  id,
  name,
  type,
  created_at,
  updated_at
FROM validated_stage