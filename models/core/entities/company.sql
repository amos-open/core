{{
  config(
    materialized='table',
    cluster_by=['industry_id'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH staging_company AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_entities_company') }}
),

validated_company AS (
  SELECT
    id,
    name,
    website,
    description,
    currency,
    industry_id,
    created_at,
    updated_at
  FROM staging_company
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
    -- Company-specific business rules
    AND {{ validate_company_business_rules('currency', 'website', 'industry_id') }}
)

SELECT
  id,
  name,
  website,
  description,
  currency,
  industry_id,
  created_at,
  updated_at
FROM validated_company