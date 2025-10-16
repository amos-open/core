{{
  config(
    materialized='incremental',
    unique_key='id',
    cluster_by=['cashflow_date', 'loan_id', 'loan_cashflow_type'],
    tags=['bi_accessible', 'canonical', 'cashflow'],
    on_schema_change='fail'
  )
}}

WITH staging_loan_cashflow AS (
  SELECT * FROM {{ ref('stg_loan_cashflow') }}
),

validated_loan_cashflow AS (
  SELECT
    id,
    loan_id,
    facility_id,
    loan_cashflow_type,
    cashflow_date,
    principal_amount,
    interest_amount,
    fee_amount,
    total_amount,
    interest_rate,
    days_in_period,
    outstanding_balance_before,
    outstanding_balance_after,
    created_at,
    updated_at
  FROM staging_loan_cashflow
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND loan_id IS NOT NULL
    AND facility_id IS NOT NULL
    AND loan_cashflow_type IS NOT NULL
    AND cashflow_date IS NOT NULL
    AND total_amount IS NOT NULL
    -- Business rule validation
    AND cashflow_date <= CURRENT_DATE()
    AND (principal_amount IS NULL OR principal_amount >= 0)
    AND (interest_amount IS NULL OR interest_amount >= 0)
    AND (fee_amount IS NULL OR fee_amount >= 0)
    AND (interest_rate IS NULL OR interest_rate >= 0)
    AND (days_in_period IS NULL OR days_in_period >= 0)
    AND (outstanding_balance_before IS NULL OR outstanding_balance_before >= 0)
    AND (outstanding_balance_after IS NULL OR outstanding_balance_after >= 0)
    -- Validate loan_cashflow_type enum values
    AND loan_cashflow_type IN (
      'DRAW',
      'PRINCIPAL_PAYMENT',
      'INTEREST_PAYMENT',
      'FEE_PAYMENT',
      'PREPAYMENT',
      'DEFAULT',
      'RECOVERY'
    )
    -- Ensure total_amount is consistent with component amounts
    AND (
      total_amount = COALESCE(principal_amount, 0) + COALESCE(interest_amount, 0) + COALESCE(fee_amount, 0)
      OR (principal_amount IS NULL AND interest_amount IS NULL AND fee_amount IS NULL)
    )
    -- Ensure balance calculations are consistent for principal payments
    AND (
      loan_cashflow_type != 'PRINCIPAL_PAYMENT' 
      OR outstanding_balance_before IS NULL 
      OR outstanding_balance_after IS NULL 
      OR principal_amount IS NULL
      OR ABS((outstanding_balance_before - principal_amount) - outstanding_balance_after) < 0.01
    )
)

SELECT
  id,
  loan_id,
  facility_id,
  loan_cashflow_type,
  cashflow_date,
  principal_amount,
  interest_amount,
  fee_amount,
  total_amount,
  interest_rate,
  days_in_period,
  outstanding_balance_before,
  outstanding_balance_after,
  created_at,
  updated_at
FROM validated_loan_cashflow

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
     OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}