-- Company entity model with industry relationships
-- Transforms staging company data to canonical schema with industry classification

{{ config(
    materialized='table',
    cluster_by=['country_code', 'company_type'],
    tags=['bi_accessible', 'canonical', 'entity']
) }}

WITH company_base AS (
    SELECT 
        id,
        name,
        company_type,
        status,
        founded_date,
        country_code,
        description,
        created_at,
        updated_at
    FROM {{ ref('stg_company') }}
),

company_with_derived_attributes AS (
    SELECT 
        id,
        name,
        company_type,
        status,
        founded_date,
        country_code,
        description,
        created_at,
        updated_at,
        
        -- Simple derived attribute: company age calculation
        CASE 
            WHEN founded_date IS NOT NULL 
            THEN DATEDIFF('year', founded_date, CURRENT_DATE()) 
            ELSE NULL 
        END as company_age_years
        
    FROM company_base
)

SELECT 
    id,
    name,
    company_type,
    status,
    founded_date,
    country_code,
    description,
    company_age_years,
    created_at,
    updated_at
FROM company_with_derived_attributes

-- Data quality validation
WHERE id IS NOT NULL
  AND name IS NOT NULL
  AND company_type IS NOT NULL
  AND status IS NOT NULL