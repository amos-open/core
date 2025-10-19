{{
  config(
    materialized='table',
    cluster_by=['base_currency_code', 'type'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_fund AS (
  SELECT * FROM {{ ref('int_entities_fund') }}
),

validated_fund AS (
  SELECT
    id,
    name,
    null as type,
    null as vintage,
    null as management_fee,
    null as hurdle,
    null as carried_interest,
    null as target_commitment,
    null as incorporated_in,
    base_currency_code,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) as created_at,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) as updated_at
  FROM staging_fund
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
    AND base_currency_code IS NOT NULL
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
  CAST(created_at AS TIMESTAMP_NTZ) as created_at,
  CAST(updated_at AS TIMESTAMP_NTZ) as updated_at
FROM validated_fund