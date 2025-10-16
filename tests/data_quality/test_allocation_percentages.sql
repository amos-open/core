-- Data Quality Test: Allocation Percentage Validation
-- Tests that allocation percentages sum correctly and follow business rules

{{ config(
    severity='warn',
    tags=['data_quality', 'allocations', 'business_rules']
) }}

-- Test allocation percentages across all bridge tables
WITH allocation_issues AS (
    -- Company Industry Allocations
    SELECT 
        'company_industry' as table_name,
        company_id::text as entity_id,
        c.name as entity_name,
        NULL as valid_from,
        SUM(allocation_pct) as total_allocation_pct,
        COUNT(*) as allocation_count,
        COUNT(CASE WHEN primary_flag = true THEN 1 END) as primary_count,
        CASE 
            WHEN ABS(SUM(allocation_pct) - 100.0) > 0.01 THEN 'Allocation percentages do not sum to 100%'
            WHEN COUNT(CASE WHEN primary_flag = true THEN 1 END) != 1 THEN 'Must have exactly one primary flag'
            WHEN MIN(allocation_pct) < 0 OR MAX(allocation_pct) > 100 THEN 'Allocation percentage out of range'
            ELSE NULL
        END as issue_description
    FROM {{ ref('company_industry') }} ci
    JOIN {{ ref('company') }} c ON ci.company_id = c.id
    GROUP BY company_id, c.name
    
    UNION ALL
    
    -- Company Country Allocations
    SELECT 
        'company_country' as table_name,
        company_id::text as entity_id,
        c.name as entity_name,
        NULL as valid_from,
        SUM(allocation_pct) as total_allocation_pct,
        COUNT(*) as allocation_count,
        COUNT(CASE WHEN primary_flag = true THEN 1 END) as primary_count,
        CASE 
            WHEN ABS(SUM(allocation_pct) - 100.0) > 0.01 THEN 'Allocation percentages do not sum to 100%'
            WHEN COUNT(CASE WHEN primary_flag = true THEN 1 END) != 1 THEN 'Must have exactly one primary flag'
            WHEN MIN(allocation_pct) < 0 OR MAX(allocation_pct) > 100 THEN 'Allocation percentage out of range'
            ELSE NULL
        END as issue_description
    FROM {{ ref('company_country') }} cc
    JOIN {{ ref('company') }} c ON cc.company_id = c.id
    GROUP BY company_id, c.name
    

)

SELECT 
    table_name,
    entity_id,
    entity_name,
    valid_from,
    total_allocation_pct,
    allocation_count,
    primary_count,
    issue_description
FROM allocation_issues
WHERE issue_description IS NOT NULL
ORDER BY table_name, entity_name