{{
  config(
    materialized='table',
    cluster_by=['investor_type_id'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_investor AS (
  SELECT * FROM {{ ref('int_entities_investor') }}
),

validated_investor AS (
  SELECT
    id,
    name,
    investor_type_id,
    created_at,
    updated_at
  FROM staging_investor
  WHERE 1=1
    -- Entity base validation
    AND id IS NOT NULL
    AND name IS NOT NULL
    AND investor_type_id IS NOT NULL
)

SELECT
  id,
  name,
  investor_type_id,
  created_at,
  updated_at
FROM validated_investor