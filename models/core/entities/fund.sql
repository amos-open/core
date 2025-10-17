{{
  config(
    materialized='table',
    cluster_by=['base_currency_code', 'type'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_fund AS (
  SELECT * FROM {{ ref(var('source_package'), 'int_entities_fund') }}
),

validated_fund AS (
  SELECT
    id,
    name,
    type,
    vintage,
    management_fee,
    hurdle,
    carried_interest,
    target_commitment,
    incorporated_in,
    base_currency_code,
    created_at,
    updated_at
  FROM staging_fund
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND name IS NOT NULL
    -- Business rule validation
    AND (management_fee IS NULL OR management_fee >= 0)
    AND (hurdle IS NULL OR hurdle >= 0)
    AND (carried_interest IS NULL OR carried_interest >= 0)
    AND (target_commitment IS NULL OR target_commitment > 0)
    AND (base_currency_code IS NULL OR LENGTH(base_currency_code) = 3)
)

SELECT
  id,
  name,
  type,
  vintage,
  management_fee,
  hurdle,
  carried_interest,
  target_commitment,
  incorporated_in,
  base_currency_code,
  created_at,
  updated_at
FROM validated_fund