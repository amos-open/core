{{
  config(
    materialized='table',
    cluster_by=['company_id', 'round_date'],
    tags=['bi_accessible', 'canonical', 'supporting']
  )
}}

WITH staging_investment_round AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_pm_investments') }}
),

validated_investment_round AS (
  SELECT
    id,
    company_id,
    round_name,
    round_type,
    round_date,
    pre_money_valuation,
    post_money_valuation,
    total_amount_raised,
    lead_investor_id,
    share_class_id,
    shares_issued,
    price_per_share,
    liquidation_preference,
    anti_dilution_protection,
    board_seats,
    created_at,
    updated_at
  FROM staging_investment_round
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND company_id IS NOT NULL
    AND round_type IS NOT NULL
    -- Business rule validation
    AND (pre_money_valuation IS NULL OR pre_money_valuation >= 0)
    AND (post_money_valuation IS NULL OR post_money_valuation >= 0)
    AND (total_amount_raised IS NULL OR total_amount_raised >= 0)
    AND (shares_issued IS NULL OR shares_issued >= 0)
    AND (price_per_share IS NULL OR price_per_share >= 0)
    AND (liquidation_preference IS NULL OR liquidation_preference >= 0)
    -- Valuation consistency check
    AND (
      pre_money_valuation IS NULL OR 
      post_money_valuation IS NULL OR 
      total_amount_raised IS NULL OR
      post_money_valuation = pre_money_valuation + total_amount_raised
    )
)

SELECT
  id,
  company_id,
  round_name,
  round_type,
  round_date,
  pre_money_valuation,
  post_money_valuation,
  total_amount_raised,
  lead_investor_id,
  share_class_id,
  shares_issued,
  price_per_share,
  liquidation_preference,
  anti_dilution_protection,
  board_seats,
  created_at,
  updated_at
FROM validated_investment_round