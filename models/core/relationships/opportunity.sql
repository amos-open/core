{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'stage_id', 'company_id'],
    tags=['bi_accessible', 'canonical', 'relationship']
  )
}}

WITH staging_opportunity AS (
  SELECT * FROM {{ ref('stg_opportunity') }}
),

validated_opportunity AS (
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
  FROM staging_opportunity
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND name IS NOT NULL
    -- Business rule validation
    AND (amount IS NULL OR amount > 0)
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