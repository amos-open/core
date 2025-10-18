{{
  config(
    materialized='incremental',
    unique_key=['instrument_id', 'as_of_date', 'source'],
    cluster_by=['as_of_date', 'instrument_id'],
    tags=['bi_accessible', 'canonical', 'snapshot'],
    on_schema_change='fail'
  )
}}

WITH staging_instrument_snapshot AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_snapshots_investment_nav') }}
),

validated_instrument_snapshot AS (
  SELECT
    id,
    instrument_id,
    as_of_date,
    currency_code,
    fx_rate,
    
    -- Common valuation fields
    fair_value,
    amortized_cost,
    fair_value_converted,
    amortized_cost_converted,
    
    -- Basic loan fields (detailed loan snapshots in PC package)
    principal_outstanding,
    undrawn_commitment,
    accrued_income,
    accrued_fees,
    principal_outstanding_converted,
    undrawn_commitment_converted,
    accrued_income_converted,
    accrued_fees_converted,
    
    -- Equity-specific fields (null for loan instruments)
    equity_stake_pct,
    equity_dividends_cum,
    equity_exit_proceeds_actual,
    equity_exit_proceeds_forecast,
    
    source,
    source_file_ref,
    created_at,
    updated_at
  FROM staging_instrument_snapshot
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND instrument_id IS NOT NULL
    AND as_of_date IS NOT NULL
    AND currency_code IS NOT NULL
    AND source IS NOT NULL
    -- Business rule validation
    AND (fair_value IS NULL OR fair_value >= 0)
    AND (amortized_cost IS NULL OR amortized_cost >= 0)
    AND (fair_value_converted IS NULL OR fair_value_converted >= 0)
    AND (amortized_cost_converted IS NULL OR amortized_cost_converted >= 0)
    AND (principal_outstanding IS NULL OR principal_outstanding >= 0)
    AND (undrawn_commitment IS NULL OR undrawn_commitment >= 0)
    AND (equity_stake_pct IS NULL OR (equity_stake_pct >= 0 AND equity_stake_pct <= 100))
    AND (equity_dividends_cum IS NULL OR equity_dividends_cum >= 0)
    AND (equity_exit_proceeds_actual IS NULL OR equity_exit_proceeds_actual >= 0)
    AND (equity_exit_proceeds_forecast IS NULL OR equity_exit_proceeds_forecast >= 0)
    -- Ensure as_of_date is not in the future
    AND as_of_date <= CURRENT_DATE()
    -- Ensure fx_rate is positive when provided
    AND (fx_rate IS NULL OR fx_rate > 0)
),

-- Add instrument type validation by joining with instrument table
instrument_type_validated AS (
  SELECT 
    s.*,
    i.instrument_type
  FROM validated_instrument_snapshot s
  LEFT JOIN {{ ref('instrument') }} i ON s.instrument_id = i.id
  WHERE 1=1
    -- Validate equity-specific fields are only populated for equity instruments
    AND (
      i.instrument_type NOT IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
      OR (
        -- For equity instruments, at least one equity field should be populated or all should be null
        equity_stake_pct IS NOT NULL 
        OR equity_dividends_cum IS NOT NULL 
        OR equity_exit_proceeds_actual IS NOT NULL 
        OR equity_exit_proceeds_forecast IS NOT NULL
        OR (
          equity_stake_pct IS NULL 
          AND equity_dividends_cum IS NULL 
          AND equity_exit_proceeds_actual IS NULL 
          AND equity_exit_proceeds_forecast IS NULL
        )
      )
    )
    -- Validate loan-specific fields are only populated for loan instruments
    AND (
      i.instrument_type != 'LOAN'
      OR (
        -- For loan instruments, at least one loan field should be populated or all should be null
        principal_outstanding IS NOT NULL 
        OR undrawn_commitment IS NOT NULL 
        OR accrued_income IS NOT NULL 
        OR accrued_fees IS NOT NULL
        OR (
          principal_outstanding IS NULL 
          AND undrawn_commitment IS NULL 
          AND accrued_income IS NULL 
          AND accrued_fees IS NULL
        )
      )
    )
)

SELECT
  id,
  instrument_id,
  as_of_date,
  currency_code,
  fx_rate,
  
  -- Common valuation fields
  fair_value,
  amortized_cost,
  fair_value_converted,
  amortized_cost_converted,
  
  -- Basic loan fields
  principal_outstanding,
  undrawn_commitment,
  accrued_income,
  accrued_fees,
  principal_outstanding_converted,
  undrawn_commitment_converted,
  accrued_income_converted,
  accrued_fees_converted,
  
  -- Equity-specific fields
  equity_stake_pct,
  equity_dividends_cum,
  equity_exit_proceeds_actual,
  equity_exit_proceeds_forecast,
  
  source,
  source_file_ref,
  created_at,
  updated_at
FROM instrument_type_validated

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE as_of_date > (SELECT COALESCE(MAX(as_of_date), '1900-01-01') FROM {{ this }})
     OR updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01'::timestamp) FROM {{ this }})
{% endif %}