{{
  config(
    materialized='table',
    cluster_by=['facility_id', 'start_date'],
    tags=['bi_accessible', 'canonical', 'relationship']
  )
}}

WITH staging_loan AS (
  SELECT * FROM {{ ref('stg_loan') }}
),

validated_loan AS (
  SELECT
    id,
    facility_id,
    loan_type,
    tranche_label,
    commitment_amount,
    currency_code,
    start_date,
    maturity_date,
    interest_index,
    index_tenor_days,
    fixed_rate_pct,
    spread_bps,
    floor_pct,
    day_count,
    pay_freq_months,
    amortization_type,
    security_rank,
    status,
    created_at,
    updated_at
  FROM staging_loan
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND facility_id IS NOT NULL
    AND loan_type IS NOT NULL
    AND currency_code IS NOT NULL
    AND start_date IS NOT NULL
    AND maturity_date IS NOT NULL
    AND interest_index IS NOT NULL
    AND spread_bps IS NOT NULL
    AND day_count IS NOT NULL
    AND pay_freq_months IS NOT NULL
    AND amortization_type IS NOT NULL
    -- Enum validation
    AND loan_type IN ('TERM', 'REVOLVER', 'DDTL', 'BRIDGE', 'MEZZ')
    AND interest_index IN ('SOFR', 'EURIBOR', 'SONIA', 'FED_FUNDS', 'FIXED')
    AND day_count IN ('30E_360', 'ACT_360', 'ACT_365', 'ACT_ACT')
    AND amortization_type IN ('BULLET', 'STRAIGHT_LINE', 'CUSTOM_SCHEDULE')
    AND (security_rank IS NULL OR security_rank IN ('SENIOR_SECURED', 'SENIOR_UNSECURED', 'SECOND_LIEN', 'MEZZANINE', 'PIK'))
    -- Business rule validation
    AND LENGTH(currency_code) = 3
    AND maturity_date > start_date
    AND (commitment_amount IS NULL OR commitment_amount > 0)
    AND (fixed_rate_pct IS NULL OR fixed_rate_pct >= 0)
    AND (floor_pct IS NULL OR floor_pct >= 0)
    AND pay_freq_months > 0
)

SELECT
  id,
  facility_id,
  loan_type,
  tranche_label,
  commitment_amount,
  currency_code,
  start_date,
  maturity_date,
  interest_index,
  index_tenor_days,
  fixed_rate_pct,
  spread_bps,
  floor_pct,
  day_count,
  pay_freq_months,
  amortization_type,
  security_rank,
  status,
  created_at,
  updated_at
FROM validated_loan