{{
  config(
    materialized='table',
    cluster_by=['company_id', 'investor_id'],
    tags=['bi_accessible', 'canonical', 'supporting']
  )
}}

WITH staging_shareholder AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_admin_investors') }}
),

validated_shareholder AS (
  SELECT
    id,
    company_id,
    investor_id,
    share_class_id,
    shares_owned,
    ownership_percentage,
    acquisition_date,
    acquisition_price_per_share,
    vesting_schedule,
    board_rights,
    information_rights,
    tag_along_rights,
    drag_along_rights,
    created_at,
    updated_at
  FROM staging_shareholder
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND company_id IS NOT NULL
    AND investor_id IS NOT NULL
    -- Business rule validation
    AND (shares_owned IS NULL OR shares_owned >= 0)
    AND (ownership_percentage IS NULL OR (ownership_percentage >= 0 AND ownership_percentage <= 100))
    AND (acquisition_price_per_share IS NULL OR acquisition_price_per_share >= 0)
)

SELECT
  id,
  company_id,
  investor_id,
  share_class_id,
  shares_owned,
  ownership_percentage,
  acquisition_date,
  acquisition_price_per_share,
  vesting_schedule,
  board_rights,
  information_rights,
  tag_along_rights,
  drag_along_rights,
  created_at,
  updated_at
FROM validated_shareholder