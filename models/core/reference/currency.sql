{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_currency AS (
  SELECT * FROM {{ ref('stg_currency') }}
),

validated_currency AS (
  SELECT
    code,
    name,
    created_at,
    updated_at
  FROM staging_currency
  WHERE 1=1
    -- Basic validation
    AND code IS NOT NULL
    AND name IS NOT NULL
    AND LENGTH(code) = 3
)

SELECT
  code,
  name,
  created_at,
  updated_at
FROM validated_currency