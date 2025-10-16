{{
  config(
    materialized='table',
    cluster_by=['company_id', 'industry_id'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_company_industry AS (
  SELECT * FROM {{ ref('stg_company_industry') }}
),

validated_company_industry AS (
  SELECT
    company_id,
    industry_id,
    primary_flag,
    allocation_pct,
    created_at,
    updated_at
  FROM staging_company_industry
  WHERE 1=1
    -- Basic validation
    AND company_id IS NOT NULL
    AND industry_id IS NOT NULL
    -- Business rule validation
    AND primary_flag IN (TRUE, FALSE)
    AND allocation_pct >= 0
    AND allocation_pct <= 100
)

SELECT
  company_id,
  industry_id,
  primary_flag,
  allocation_pct,
  created_at,
  updated_at
FROM validated_company_industry