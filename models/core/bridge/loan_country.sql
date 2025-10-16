{{
  config(
    materialized='table',
    cluster_by=['loan_id', 'country_code', 'valid_from'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_loan_country AS (
  SELECT * FROM {{ ref('stg_loan_country') }}
),

validated_loan_country AS (
  SELECT
    loan_id,
    country_code,
    allocation_pct,
    valid_from,
    valid_to,
    created_at,
    updated_at
  FROM staging_loan_country
  WHERE 1=1
    -- Basic validation
    AND loan_id IS NOT NULL
    AND country_code IS NOT NULL
    AND valid_from IS NOT NULL
    -- Business rule validation
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    AND LENGTH(country_code) = 2
    -- Temporal validation
    AND (valid_to IS NULL OR valid_to > valid_from)
)

SELECT
  loan_id,
  country_code,
  allocation_pct,
  valid_from,
  valid_to,
  created_at,
  updated_at
FROM validated_loan_country