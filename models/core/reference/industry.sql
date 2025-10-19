{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_industry AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_ref_industries') }}
),

validated_industry AS (
  SELECT
    id,
    industry_name as name,
    CAST(created_date AS TIMESTAMP_NTZ) as created_at,
    CAST(last_modified_date AS TIMESTAMP_NTZ) as updated_at
  FROM staging_industry
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND industry_name IS NOT NULL
)

SELECT
  id,
  name,
  created_at,
  updated_at
FROM validated_industry