{{
  config(
    materialized='table',
    cluster_by=['company_id', 'share_class_type'],
    tags=['bi_accessible', 'canonical', 'supporting']
  )
}}

WITH staging_share_class AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_pm_investments') }}
),

validated_share_class AS (
  SELECT
    id,
    company_id,
    share_class_type,
    share_class_name,
    par_value,
    liquidation_preference,
    dividend_rate,
    voting_rights,
    anti_dilution_protection,
    participation_rights,
    conversion_ratio,
    created_at,
    updated_at
  FROM staging_share_class
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND company_id IS NOT NULL
    AND share_class_type IS NOT NULL
    -- Business rule validation
    AND (par_value IS NULL OR par_value >= 0)
    AND (liquidation_preference IS NULL OR liquidation_preference >= 0)
    AND (dividend_rate IS NULL OR dividend_rate >= 0)
    AND (conversion_ratio IS NULL OR conversion_ratio > 0)
)

SELECT
  id,
  company_id,
  share_class_type,
  share_class_name,
  par_value,
  liquidation_preference,
  dividend_rate,
  voting_rights,
  anti_dilution_protection,
  participation_rights,
  conversion_ratio,
  created_at,
  updated_at
FROM validated_share_class