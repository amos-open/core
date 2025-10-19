{{
  config(
    materialized='table',
    cluster_by=['fund_id', 'instrument_type'],
    tags=['core', 'instrument', 'bi_accessible', 'canonical']
  )
}}

WITH staging_instrument AS (
  SELECT * FROM {{ ref('int_relationships_fund_investment') }}
),

validated_instrument AS (
  SELECT
    -- Truncate id to 36 chars to satisfy varchar(36) contract
    SUBSTR(relationship_id, 1, 36) as id,
    canonical_fund_id as fund_id,
    canonical_company_id as company_id,
    investment_type as instrument_type,
    'USD' as base_currency_code,
    investment_date as inception_date,
    target_exit_date as termination_date,
    investment_thesis as description,
    CAST(created_date AS timestamp_ntz) as created_at,
    CAST(last_modified_date AS timestamp_ntz) as updated_at
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