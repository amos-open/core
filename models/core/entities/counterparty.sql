{{
  config(
    materialized='table',
    cluster_by=['type', 'country_code'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_counterparty AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_entities_counterparty') }}
),

validated_counterparty AS (
  SELECT
    id,
    name,
    type,
    country_code,
    created_at,
    updated_at
  FROM staging_counterparty
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND name IS NOT NULL
    -- Business rule validation
    AND (country_code IS NULL OR LENGTH(country_code) = 2)
)

SELECT
  id,
  name,
  type,
  country_code,
  created_at,
  updated_at
FROM validated_counterparty