{{
  config(
    materialized='table',
    cluster_by=['industry_id'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_company AS (
  SELECT * FROM {{ ref('stg_company') }}
),

validated_company AS (
  SELECT
    id,
    name,
    website,
    description,
    currency,
    industry_id,
    created_at,
    updated_at
  FROM staging_company
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND name IS NOT NULL
    -- Business rule validation
    AND (currency IS NULL OR LENGTH(currency) = 3)
)

SELECT
  id,
  name,
  website,
  description,
  currency,
  industry_id,
  created_at,
  updated_at
FROM validated_company