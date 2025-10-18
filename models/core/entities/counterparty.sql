{{
  config(
    materialized='table',
    cluster_by=['type', 'country_code'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_counterparty AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_entities_counterparty') }}
),

validated_counterparty AS (
  SELECT
    id,
    name,
    type,
    country_code,
    created_at,
    updated_at
  FROM staging_counterparty
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
    -- Counterparty-specific business rules
    AND {{ validate_counterparty_business_rules('type', 'country_code') }}
)

SELECT
  id,
  name,
  type,
  country_code,
  created_at,
  updated_at
FROM validated_counterparty