{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'instrument_type'],
    tags=['core', 'instrument', 'bi_accessible', 'canonical']
  )
}}

WITH staging_instrument AS (
  SELECT * FROM {{ ref(var('source_package'), 'int_relationships_fund_investment') }}
),

validated_instrument AS (
  SELECT
    id,
    fund_id,
    company_id,
    instrument_type,
    base_currency_code,
    inception_date,
    termination_date,
    description,
    created_at,
    updated_at
  FROM staging_instrument
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND instrument_type IS NOT NULL
    AND base_currency_code IS NOT NULL
    -- Equity instruments must have company_id
    AND (
      instrument_type NOT IN ('EQUITY', 'CONVERTIBLE', 'WARRANT') 
      OR company_id IS NOT NULL
    )
)

SELECT
  id,
  fund_id,
  company_id,
  instrument_type,
  base_currency_code,
  inception_date,
  termination_date,
  description,
  created_at,
  updated_at
FROM validated_instrument