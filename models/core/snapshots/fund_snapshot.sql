{{
  config(
    materialized='incremental',
    unique_key=['fund_id', 'as_of_date'],
    cluster_by=['as_of_date', 'fund_id'],
    tags=['bi_accessible', 'canonical', 'snapshot'],
    on_schema_change='fail'
  )
}}

WITH staging_fund_snapshot AS (
  SELECT * FROM {{ ref('int_snapshots_fund_nav') }}
),

validated_fund_snapshot AS (
  SELECT
    canonical_fund_id as fund_id,
    snapshot_date as as_of_date,
    total_nav_usd as total_nav,
    committed_capital_usd as total_commitment,
    called_capital_usd as total_called,
    distributed_capital_usd as total_distributed,
    dpi_ratio as dpi,
    rvpi_ratio as rvpi,
    CAST(null AS NUMBER) as expected_coc,
    CAST(created_date AS timestamp_ntz) as created_at,
    CAST(last_modified_date AS timestamp_ntz) as updated_at
  FROM staging_fund_snapshot
  WHERE 1=1
    -- Basic validation
    AND fund_id IS NOT NULL
    AND as_of_date IS NOT NULL
    -- Business rule validation
    AND (total_nav IS NULL OR total_nav >= 0)
    AND (total_commitment IS NULL OR total_commitment >= 0)
    AND (total_called IS NULL OR total_called >= 0)
    AND (total_distributed IS NULL OR total_distributed >= 0)
    AND (dpi IS NULL OR dpi >= 0)
    AND (rvpi IS NULL OR rvpi >= 0)
    AND (expected_coc IS NULL OR expected_coc >= 0)
    -- Ensure as_of_date is not in the future
    AND as_of_date <= CURRENT_DATE()
    -- Ensure total_called <= total_commitment (if both are not null)
    AND (total_commitment IS NULL OR total_called IS NULL OR total_called <= total_commitment)
)

SELECT
  fund_id,
  as_of_date,
  total_nav,
  total_commitment,
  total_called,
  total_distributed,
  dpi,
  rvpi,
  expected_coc,
  created_at,
  updated_at
FROM validated_fund_snapshot

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE as_of_date > (SELECT MAX(as_of_date) FROM {{ this }})
     OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}