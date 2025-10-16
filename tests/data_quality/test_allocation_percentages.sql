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
    
    UNION ALL
    
    -- Facility Lender Allocations
    SELECT 
        'facility_lender' as table_name,
        facility_id::text as entity_id,
        f.name || ' - ' || fac.facility_type as entity_name,
        NULL as valid_from,
        SUM(allocation_pct) as total_allocation_pct,
        COUNT(*) as allocation_count,
        COUNT(CASE WHEN syndicate_role = 'AGENT' THEN 1 END) as primary_count,
        CASE 
            WHEN ABS(SUM(allocation_pct) - 100.0) > 0.01 THEN 'Allocation percentages do not sum to 100%'
            WHEN COUNT(CASE WHEN syndicate_role = 'AGENT' THEN 1 END) > 1 THEN 'Multiple agents not allowed'
            WHEN MIN(allocation_pct) < 0 OR MAX(allocation_pct) > 100 THEN 'Allocation percentage out of range'
            ELSE NULL
        END as issue_description
    FROM {{ ref('facility_lender') }} fl
    JOIN {{ ref('facility') }} fac ON fl.facility_id = fac.id
    JOIN {{ ref('fund') }} f ON fac.fund_id = f.id
    GROUP BY facility_id, f.name, fac.facility_type
    
    UNION ALL
    
    -- Loan Country Allocations (Current only - valid_to IS NULL)
    SELECT 
        'loan_country' as table_name,
        loan_id::text as entity_id,
        f.name || ' - ' || l.loan_type as entity_name,
        lc.valid_from,
        SUM(allocation_pct) as total_allocation_pct,
        COUNT(*) as allocation_count,
        0 as primary_count,  -- No primary flag for loan allocations
        CASE 
            WHEN ABS(SUM(allocation_pct) - 100.0) > 0.01 THEN 'Allocation percentages do not sum to 100%'
            WHEN MIN(allocation_pct) < 0 OR MAX(allocation_pct) > 100 THEN 'Allocation percentage out of range'
            ELSE NULL
        END as issue_description
    FROM {{ ref('loan_country') }} lc
    JOIN {{ ref('loan') }} l ON lc.loan_id = l.id
    JOIN {{ ref('facility') }} fac ON l.facility_id = fac.id
    JOIN {{ ref('fund') }} f ON fac.fund_id = f.id
    WHERE lc.valid_to IS NULL  -- Current allocations only
    GROUP BY loan_id, f.name, l.loan_type, lc.valid_from
    
    UNION ALL
    
    -- Loan Industry Allocations (Current only - valid_to IS NULL)
    SELECT 
        'loan_industry' as table_name,
        loan_id::text as entity_id,
        f.name || ' - ' || l.loan_type as entity_name,
        li.valid_from,
        SUM(allocation_pct) as total_allocation_pct,
        COUNT(*) as allocation_count,
        0 as primary_count,  -- No primary flag for loan allocations
        CASE 
            WHEN ABS(SUM(allocation_pct) - 100.0) > 0.01 THEN 'Allocation percentages do not sum to 100%'
            WHEN MIN(allocation_pct) < 0 OR MAX(allocation_pct) > 100 THEN 'Allocation percentage out of range'
            ELSE NULL
        END as issue_description
    FROM {{ ref('loan_industry') }} li
    JOIN {{ ref('loan') }} l ON li.loan_id = l.id
    JOIN {{ ref('facility') }} fac ON l.facility_id = fac.id
    JOIN {{ ref('fund') }} f ON fac.fund_id = f.id
    WHERE li.valid_to IS NULL  -- Current allocations only
    GROUP BY loan_id, f.name, l.loan_type, li.valid_from
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