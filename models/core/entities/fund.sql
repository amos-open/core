{{
  config(
    materialized='table',
    cluster_by=['base_currency_code', 'type'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_fund AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_entities_fund') }}
),

validated_fund AS (
  SELECT
    id,
    name,
    type,
    vintage,
    management_fee,
    hurdle,
    carried_interest,
    target_commitment,
    incorporated_in,
    base_currency_code,
    created_at,
    updated_at
  FROM staging_fund
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
    -- Fund-specific business rules
    AND {{ validate_fund_business_rules('management_fee', 'hurdle', 'carried_interest', 'target_commitment', 'base_currency_code', 'vintage') }}
    -- Fund type validation
    AND {{ validate_fund_type('type') }}
)

SELECT
  id,
  name,
  type,
  vintage,
  management_fee,
  hurdle,
  carried_interest,
  target_commitment,
  incorporated_in,
  base_currency_code,
  created_at,
  updated_at
FROM validated_fund