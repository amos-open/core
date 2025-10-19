{{
  config(
    materialized='table',
    tags=['bi_accessible', 'canonical', 'dimension']
  )
}}

WITH staging_investor_type AS (
  SELECT * FROM {{ ref('amos_source_example', 'stg_admin_investors') }}
),

validated_investor_type AS (
  SELECT
    id,  -- Generated in staging from investor_code
    INVESTOR_NAME as name,
    STANDARDIZED_INVESTOR_TYPE as kyc_category,
    CAST(CREATED_DATE AS TIMESTAMP_NTZ) as created_at,
    CAST(LAST_MODIFIED_DATE AS TIMESTAMP_NTZ) as updated_at
  FROM staging_investor_type
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
)

SELECT
  id,
  name,
  kyc_category,
  created_at,
  updated_at
FROM validated_investor_type