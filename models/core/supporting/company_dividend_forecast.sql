{{
  config(
    materialized='table',
    cluster_by=['company_id', 'forecast_date'],
    tags=['bi_accessible', 'canonical', 'supporting']
  )
}}

WITH staging_company_dividend_forecast AS (
  SELECT * FROM {{ ref('stg_company_dividend_forecast') }}
),

validated_company_dividend_forecast AS (
  SELECT
    id,
    company_id,
    forecast_date,
    forecast_period_start,
    forecast_period_end,
    dividend_per_share,
    total_dividend_amount,
    dividend_currency_code,
    dividend_type,
    payment_frequency,
    confidence_level,
    forecast_method,
    forecast_assumptions,
    created_at,
    updated_at
  FROM staging_company_dividend_forecast
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND company_id IS NOT NULL
    AND forecast_date IS NOT NULL
    -- Business rule validation
    AND (dividend_per_share IS NULL OR dividend_per_share >= 0)
    AND (total_dividend_amount IS NULL OR total_dividend_amount >= 0)
    AND (dividend_currency_code IS NULL OR LENGTH(dividend_currency_code) = 3)
    AND (confidence_level IS NULL OR (confidence_level >= 0 AND confidence_level <= 100))
    -- Period validity check
    AND (forecast_period_start IS NULL OR forecast_period_end IS NULL OR forecast_period_start <= forecast_period_end)
)

SELECT
  id,
  company_id,
  forecast_date,
  forecast_period_start,
  forecast_period_end,
  dividend_per_share,
  total_dividend_amount,
  dividend_currency_code,
  dividend_type,
  payment_frequency,
  confidence_level,
  forecast_method,
  forecast_assumptions,
  created_at,
  updated_at
FROM validated_company_dividend_forecast