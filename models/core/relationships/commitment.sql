{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'investor_id'],
    tags=['bi_accessible', 'canonical', 'relationship']
  )
}}

WITH staging_commitment AS (
  SELECT * FROM {{ ref('stg_commitment') }}
),

validated_commitment AS (
  SELECT
    id,
    fund_id,
    investor_id,
    created_at,
    updated_at
  FROM staging_commitment
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND investor_id IS NOT NULL
)

SELECT
  id,
  fund_id,
  investor_id,
  created_at,
  updated_at
FROM validated_commitment