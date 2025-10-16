{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_country AS (
  SELECT * FROM {{ ref('stg_country') }}
),

validated_country AS (
  SELECT
    code,
    name,
    region,
    subregion,
    created_at,
    updated_at
  FROM staging_country
  WHERE 1=1
    -- Basic validation
    AND code IS NOT NULL
    AND name IS NOT NULL
    AND LENGTH(code) = 2
)

SELECT
  code,
  name,
  region,
  subregion,
  created_at,
  updated_at
FROM validated_country