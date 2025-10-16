{{
  config(
    materialized='table',
    cluster_by=['loan_id', 'industry_id', 'valid_from'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_loan_industry AS (
  SELECT * FROM {{ ref('stg_loan_industry') }}
),

validated_loan_industry AS (
  SELECT
    loan_id,
    industry_id,
    allocation_pct,
    valid_from,
    valid_to,
    created_at,
    updated_at
  FROM staging_loan_industry
  WHERE 1=1
    -- Basic validation
    AND loan_id IS NOT NULL
    AND industry_id IS NOT NULL
    AND valid_from IS NOT NULL
    -- Business rule validation
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    -- Temporal validation
    AND (valid_to IS NULL OR valid_to > valid_from)
)

SELECT
  loan_id,
  industry_id,
  allocation_pct,
  valid_from,
  valid_to,
  created_at,
  updated_at
FROM validated_loan_industry