{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_currency AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_ref_currencies') }}
),

validated_currency AS (
  SELECT
    currency_code as code,
    currency_name as name,
    CAST(created_date AS TIMESTAMP_NTZ) as created_at,
    CAST(last_modified_date AS TIMESTAMP_NTZ) as updated_at
  FROM staging_currency
  WHERE 1=1
    -- Basic validation
    AND currency_code IS NOT NULL
    AND currency_name IS NOT NULL
    AND LENGTH(currency_code) = 3
)

SELECT
  code,
  name,
  created_at,
  updated_at
FROM validated_currency