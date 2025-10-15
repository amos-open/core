{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'borrower_company_id', 'agreement_date'],
    tags=['bi_accessible', 'canonical', 'relationship']
  )
}}

WITH staging_facility AS (
  SELECT * FROM {{ ref('stg_facility') }}
),

validated_facility AS (
  SELECT
    id,
    fund_id,
    investment_id,
    borrower_company_id,
    facility_type,
    agent_counterparty_id,
    agreement_date,
    effective_date,
    maturity_date,
    base_currency_code,
    total_commitment,
    purpose,
    created_at,
    updated_at
  FROM staging_facility
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND borrower_company_id IS NOT NULL
    AND facility_type IS NOT NULL
    AND agreement_date IS NOT NULL
    AND base_currency_code IS NOT NULL
    -- Enum validation for facility_type
    AND facility_type IN ('TERM_LOAN_B', 'UNITRANCHE', 'REVOLVER', 'DELAYED_DRAWDOWN', 'MEZZANINE', 'RCF', 'BRIDGE')
    -- Business rule validation
    AND LENGTH(base_currency_code) = 3
    AND (maturity_date IS NULL OR maturity_date > agreement_date)
    AND (effective_date IS NULL OR effective_date >= agreement_date)
    AND (total_commitment IS NULL OR total_commitment > 0)
)

SELECT
  id,
  fund_id,
  investment_id,
  borrower_company_id,
  facility_type,
  agent_counterparty_id,
  agreement_date,
  effective_date,
  maturity_date,
  base_currency_code,
  total_commitment,
  purpose,
  created_at,
  updated_at
FROM validated_facility