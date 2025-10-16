{{
  config(
    materialized='table',
    cluster_by=['loan_id', 'period_start_date'],
    tags=['bi_accessible', 'canonical', 'supporting']
  )
}}

WITH staging_loan_interest_period AS (
  SELECT * FROM {{ ref('stg_loan_interest_period') }}
),

validated_loan_interest_period AS (
  SELECT
    id,
    loan_id,
    period_start_date,
    period_end_date,
    days_in_period,
    interest_rate,
    base_rate,
    margin,
    outstanding_principal,
    interest_accrued,
    interest_paid,
    interest_outstanding,
    compounding_frequency,
    day_count_convention,
    rate_reset_date,
    calculation_method,
    created_at,
    updated_at
  FROM staging_loan_interest_period
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND loan_id IS NOT NULL
    AND period_start_date IS NOT NULL
    AND period_end_date IS NOT NULL
    -- Business rule validation
    AND period_start_date <= period_end_date
    AND (days_in_period IS NULL OR days_in_period > 0)
    AND (interest_rate IS NULL OR interest_rate >= 0)
    AND (base_rate IS NULL OR base_rate >= 0)
    AND (margin IS NULL OR margin >= 0)
    AND (outstanding_principal IS NULL OR outstanding_principal >= 0)
    AND (interest_accrued IS NULL OR interest_accrued >= 0)
    AND (interest_paid IS NULL OR interest_paid >= 0)
    AND (interest_outstanding IS NULL OR interest_outstanding >= 0)
    -- Interest rate consistency check
    AND (
      interest_rate IS NULL OR 
      base_rate IS NULL OR 
      margin IS NULL OR
      ABS(interest_rate - (base_rate + margin)) < 0.0001
    )
),

calculated_loan_interest_period AS (
  SELECT
    *,
    -- Calculate interest accrual based on outstanding principal and rate
    CASE 
      WHEN outstanding_principal IS NOT NULL 
           AND interest_rate IS NOT NULL 
           AND days_in_period IS NOT NULL
      THEN ROUND(
        outstanding_principal * (interest_rate / 100) * (days_in_period / 365.0), 
        2
      )
      ELSE interest_accrued
    END AS calculated_interest_accrued,
    
    -- Calculate interest outstanding as accrued minus paid
    CASE 
      WHEN interest_accrued IS NOT NULL AND interest_paid IS NOT NULL
      THEN interest_accrued - interest_paid
      ELSE interest_outstanding
    END AS calculated_interest_outstanding
  FROM validated_loan_interest_period
)

SELECT
  id,
  loan_id,
  period_start_date,
  period_end_date,
  days_in_period,
  interest_rate,
  base_rate,
  margin,
  outstanding_principal,
  -- Use calculated values if original values are missing
  COALESCE(interest_accrued, calculated_interest_accrued) as interest_accrued,
  interest_paid,
  COALESCE(interest_outstanding, calculated_interest_outstanding) as interest_outstanding,
  compounding_frequency,
  day_count_convention,
  rate_reset_date,
  calculation_method,
  created_at,
  updated_at
FROM calculated_loan_interest_period