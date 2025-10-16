{{
  config(
    materialized='incremental',
    unique_key=['loan_id', 'as_of_date'],
    cluster_by=['as_of_date', 'facility_id'],
    tags=['bi_accessible', 'canonical', 'snapshot'],
    on_schema_change='fail'
  )
}}

WITH staging_loan_snapshot AS (
  SELECT * FROM {{ ref('stg_loan_snapshot') }}
),

validated_loan_snapshot AS (
  SELECT
    loan_id,
    facility_id,
    as_of_date,
    outstanding_principal,
    accrued_interest,
    total_exposure,
    current_ltv,
    days_past_due,
    risk_rating,
    provision_amount,
    created_at,
    updated_at
  FROM staging_loan_snapshot
  WHERE 1=1
    -- Basic validation
    AND loan_id IS NOT NULL
    AND facility_id IS NOT NULL
    AND as_of_date IS NOT NULL
    -- Business rule validation
    AND (outstanding_principal IS NULL OR outstanding_principal >= 0)
    AND (accrued_interest IS NULL OR accrued_interest >= 0)
    AND (total_exposure IS NULL OR total_exposure >= 0)
    AND (current_ltv IS NULL OR (current_ltv >= 0 AND current_ltv <= 200)) -- Allow up to 200% LTV
    AND (days_past_due IS NULL OR days_past_due >= 0)
    AND (provision_amount IS NULL OR provision_amount >= 0)
    -- Ensure as_of_date is not in the future
    AND as_of_date <= CURRENT_DATE()
    -- Ensure total_exposure is consistent with principal + interest
    AND (total_exposure IS NULL OR outstanding_principal IS NULL OR accrued_interest IS NULL
         OR ABS(total_exposure - (outstanding_principal + accrued_interest)) < 0.01)
)

SELECT
  loan_id,
  facility_id,
  as_of_date,
  outstanding_principal,
  accrued_interest,
  total_exposure,
  current_ltv,
  days_past_due,
  risk_rating,
  provision_amount,
  created_at,
  updated_at
FROM validated_loan_snapshot

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE as_of_date > (SELECT MAX(as_of_date) FROM {{ this }})
     OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}