{{
  config(
    materialized='table',
    cluster_by=['industry_id'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH intermediate_company AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_entities_company') }}
),

validated_company AS (
  SELECT
    {{ amos_source_example.alias_intermediate_columns('company') }}
  FROM intermediate_company
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
    -- Company-specific business rules
    AND {{ validate_company_business_rules('currency', 'website', 'industry_id') }}
)

SELECT
  {{ amos_source_example.alias_intermediate_columns('company') }}
FROM validated_company