{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'stage_id', 'company_id'],
    tags=['bi_accessible', 'canonical', 'relationship']
  )
}}

WITH staging_opportunity AS (
  SELECT * FROM {{ ref('stg_crm_opportunities') }}
),

validated_opportunity AS (
  SELECT
    id,
    COALESCE(NULL, 'FUND-UNKNOWN') as fund_id,  -- Provide default to avoid NULL in non-nullable column
    opportunity_name as name,
    COALESCE(NULL, 'STAGE-UNKNOWN') as stage_id,  -- Provide default to avoid NULL in non-nullable column
    company_id,
    deal_owner_name as responsible,
    expected_amount as amount,
    NULL as next_step,
    opportunity_source as source,
    expected_close_date as close_date,
    CAST(created_date AS timestamp_ntz) as created_at,
    CAST(last_modified_date AS timestamp_ntz) as updated_at
  FROM staging_opportunity
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND opportunity_name IS NOT NULL
    -- Business rule validation
    AND (expected_amount IS NULL OR expected_amount > 0)
)

SELECT
  id,
  fund_id,
  name,
  stage_id,
  company_id,
  responsible,
  amount,
  next_step,
  source,
  close_date,
  created_at,
  updated_at
FROM validated_opportunity