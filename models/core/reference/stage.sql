{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_stage AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_crm_opportunities') }}
),

validated_stage AS (
  SELECT
    CAST(id AS VARCHAR) as id,
    stage as name,
    deal_type as type,
    CAST(created_date AS timestamp_ntz) as created_at,
    CAST(last_modified_date AS timestamp_ntz) as updated_at
  FROM staging_stage
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
)

SELECT
  id,
  name,
  type,
  created_at,
  updated_at
FROM validated_stage