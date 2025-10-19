{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'effective_from'],
    tags=['bi_accessible', 'canonical', 'supporting'],
    enabled=false
  )
}}

WITH staging_valuation_policy AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_pm_valuations') }}
),

validated_valuation_policy AS (
  SELECT
    id,
    fund_id,
    policy_name,
    policy_version,
    valuation_methodology,
    frequency,
    effective_from,
    effective_to,
    discount_rates,
    multiples_approach,
    dcf_assumptions,
    market_approach_criteria,
    illiquidity_discount,
    control_premium,
    approval_process,
    external_validation_required,
    policy_document_url,
    created_at,
    updated_at
  FROM staging_valuation_policy
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND policy_name IS NOT NULL
    AND valuation_methodology IS NOT NULL
    AND effective_from IS NOT NULL
    -- Business rule validation
    AND (illiquidity_discount IS NULL OR (illiquidity_discount >= 0 AND illiquidity_discount <= 100))
    AND (control_premium IS NULL OR control_premium >= 0)
    -- Temporal validity check
    AND (effective_to IS NULL OR effective_from <= effective_to)
)

SELECT
  id,
  fund_id,
  policy_name,
  policy_version,
  valuation_methodology,
  frequency,
  effective_from,
  effective_to,
  discount_rates,
  multiples_approach,
  dcf_assumptions,
  market_approach_criteria,
  illiquidity_discount,
  control_premium,
  approval_process,
  external_validation_required,
  policy_document_url,
  created_at,
  updated_at
FROM validated_valuation_policy