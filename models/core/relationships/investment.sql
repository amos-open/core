{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'company_id'],
    tags=['bi_accessible', 'canonical', 'relationship']
  )
}}

WITH staging_investment AS (
  SELECT * FROM {{ ref('stg_investment') }}
),

validated_investment AS (
  SELECT
    id,
    fund_id,
    company_id,
    description,
    investment_type,
    created_at,
    updated_at
  FROM staging_investment
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND company_id IS NOT NULL
)

SELECT
  id,
  fund_id,
  company_id,
  description,
  investment_type,
  created_at,
  updated_at
FROM validated_investment