{{
  config(
    materialized='table',
    cluster_by=['company_id', 'valuation_date'],
    tags=['bi_accessible', 'canonical', 'supporting']
  )
}}

WITH staging_company_valuation AS (
  SELECT * FROM {{ ref('stg_company_valuation') }}
),

validated_company_valuation AS (
  SELECT
    id,
    company_id,
    valuation_date,
    valuation_type,
    valuation_amount,
    valuation_currency_code,
    valuation_method,
    valuation_policy_id,
    external_valuer,
    valuation_notes,
    effective_from,
    effective_to,
    created_at,
    updated_at
  FROM staging_company_valuation
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND company_id IS NOT NULL
    AND valuation_date IS NOT NULL
    AND valuation_type IS NOT NULL
    -- Business rule validation
    AND (valuation_amount IS NULL OR valuation_amount >= 0)
    AND (valuation_currency_code IS NULL OR LENGTH(valuation_currency_code) = 3)
    -- Temporal validity check
    AND (effective_from IS NULL OR effective_to IS NULL OR effective_from <= effective_to)
)

SELECT
  id,
  company_id,
  valuation_date,
  valuation_type,
  valuation_amount,
  valuation_currency_code,
  valuation_method,
  valuation_policy_id,
  external_valuer,
  valuation_notes,
  effective_from,
  effective_to,
  created_at,
  updated_at
FROM validated_company_valuation