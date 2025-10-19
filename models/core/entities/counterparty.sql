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
    counterparty_type as type,
    country_code,
    CAST(created_date AS timestamp_ntz) as created_at,
    CAST(last_modified_date AS timestamp_ntz) as updated_at
  FROM staging_counterparty
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
    -- Counterparty-specific business rules
    AND {{ validate_counterparty_business_rules('counterparty_type', 'country_code') }}
)

SELECT
  id,
  name,
  type,
  country_code,
  created_at,
  updated_at
FROM validated_counterparty