{{
  config(
    materialized='incremental',
    unique_key=['investment_id', 'as_of_date'],
    cluster_by=['as_of_date', 'fund_id'],
    tags=['bi_accessible', 'canonical', 'snapshot'],
    on_schema_change='fail'
  )
}}

WITH staging_investment_snapshot AS (
  SELECT * FROM {{ ref('stg_investment_snapshot') }}
),

validated_investment_snapshot AS (
  SELECT
    investment_id,
    fund_id,
    company_id,
    as_of_date,
    nav,
    cost_basis,
    unrealized_gain_loss,
    ownership_percentage,
    shares_outstanding,
    share_price,
    created_at,
    updated_at
  FROM staging_investment_snapshot
  WHERE 1=1
    -- Basic validation
    AND investment_id IS NOT NULL
    AND fund_id IS NOT NULL
    AND company_id IS NOT NULL
    AND as_of_date IS NOT NULL
    -- Business rule validation
    AND (nav IS NULL OR nav >= 0)
    AND (cost_basis IS NULL OR cost_basis >= 0)
    AND (ownership_percentage IS NULL OR (ownership_percentage >= 0 AND ownership_percentage <= 100))
    AND (shares_outstanding IS NULL OR shares_outstanding >= 0)
    AND (share_price IS NULL OR share_price >= 0)
    -- Ensure as_of_date is not in the future
    AND as_of_date <= CURRENT_DATE()
    -- Ensure NAV and cost_basis relationship makes sense
    AND (nav IS NULL OR cost_basis IS NULL OR unrealized_gain_loss IS NULL 
         OR ABS((nav - cost_basis) - unrealized_gain_loss) < 0.01)
)

SELECT
  investment_id,
  fund_id,
  company_id,
  as_of_date,
  nav,
  cost_basis,
  unrealized_gain_loss,
  ownership_percentage,
  shares_outstanding,
  share_price,
  created_at,
  updated_at
FROM validated_investment_snapshot

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE as_of_date > (SELECT MAX(as_of_date) FROM {{ this }})
     OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}