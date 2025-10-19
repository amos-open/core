{{
  config(
    materialized='table',
    cluster_by=['industry_id'],
    tags=['bi_accessible', 'canonical', 'entity']
  )
}}

WITH intermediate_company AS (
  SELECT * FROM {{ ref('int_entities_company') }}
),

validated_company AS (
  SELECT
    id,
    name,
    CAST(created_at AS TIMESTAMP_NTZ) as created_at,
    CAST(updated_at AS TIMESTAMP_NTZ) as updated_at
  FROM intermediate_company
  WHERE 1=1
    -- Entity base validation
    AND {{ validate_entity_base_fields('id', 'name') }}
)

SELECT
  id,
  name,
  created_at,
  updated_at
FROM validated_company