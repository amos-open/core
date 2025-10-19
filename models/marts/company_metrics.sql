-- Company metrics mart
-- Contains complex business metrics calculated from multiple sources
-- Final BI-ready model combining company entities with calculated metrics

{{ config(
    materialized='table',
    tags=['bi_accessible', 'mart', 'metrics']
) }}

WITH company_base AS (
    SELECT 
        id,
        name,
        null as company_type,
        null as status
    FROM {{ ref('company') }}
),

-- Placeholder for valuation calculations (would join with valuation tables)
company_valuations AS (
    SELECT 
        id,
        NULL as latest_valuation,
        NULL as latest_valuation_date
    FROM company_base
),

-- Placeholder for employee data (would join with HR/employee tables)
company_employees AS (
    SELECT 
        id,
        NULL as employee_count
    FROM company_base
),

-- Placeholder for revenue data (would join with financial tables)
company_financials AS (
    SELECT 
        id,
        NULL as annual_revenue
    FROM company_base
)

SELECT 
    c.id,
    c.name,
    c.company_type,
    c.status,
    v.latest_valuation,
    v.latest_valuation_date,
    e.employee_count,
    f.annual_revenue,
    CURRENT_TIMESTAMP() as calculated_at
FROM company_base c
LEFT JOIN company_valuations v ON c.id = v.id
LEFT JOIN company_employees e ON c.id = e.id  
LEFT JOIN company_financials f ON c.id = f.id