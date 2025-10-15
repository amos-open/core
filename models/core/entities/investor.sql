-- Investor entity model with type classification
-- Transforms staging investor data to canonical schema with business constraints

{{ config(
    materialized='table',
    cluster_by=['investor_type', 'country_code'],
    tags=['bi_accessible', 'canonical', 'entity']
) }}

WITH investor_base AS (
    SELECT 
        id,
        name,
        investor_type,
        status,
        country_code,
        description,
        created_at,
        updated_at
    FROM {{ ref('stg_investor') }}
),

SELECT 
    id,
    name,
    investor_type,
    status,
    country_code,
    description,
    created_at,
    updated_at
FROM investor_base

-- Data quality validation
WHERE id IS NOT NULL
  AND name IS NOT NULL
  AND investor_type IS NOT NULL
  AND status IS NOT NULL