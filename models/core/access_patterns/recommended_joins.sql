-- Canonical Entity Relationship Patterns
-- Standard patterns for joining canonical entities while respecting security boundaries

-- =============================================================================
-- FUND-CENTRIC JOINS
-- =============================================================================

-- Pattern 1: Fund with Latest Performance Metrics
-- Use this pattern for fund performance analysis
/*
SELECT 
    f.id,
    f.name,
    f.vintage,
    f.type,
    c.name as currency_name,
    fs.as_of_date,
    fs.total_nav,
    fs.dpi,
    fs.rvpi,
    fs.expected_coc
FROM {{ ref('fund') }} f
JOIN {{ ref('currency') }} c ON f.base_currency_code = c.code
JOIN {{ ref('fund_snapshot') }} fs ON f.id = fs.fund_id
WHERE fs.as_of_date = (
    SELECT MAX(as_of_date) 
    FROM {{ ref('fund_snapshot') }} fs2 
    WHERE fs2.fund_id = f.id
)
ORDER BY f.vintage DESC, f.name;
*/

-- Pattern 2: Fund with Investor Commitments
-- Use this pattern for LP analysis and reporting
/*
SELECT 
    f.id as fund_id,
    f.name as fund_name,
    f.vintage,
    inv.id as investor_id,
    inv.name as investor_name,
    it.name as investor_type,
    com.id as commitment_id
FROM {{ ref('fund') }} f
JOIN {{ ref('commitment') }} com ON f.id = com.fund_id
JOIN {{ ref('investor') }} inv ON com.investor_id = inv.id
JOIN {{ ref('investor_type') }} it ON inv.investor_type_id = it.id
ORDER BY f.vintage DESC, inv.name;
*/

-- =============================================================================
-- INVESTMENT-CENTRIC JOINS
-- =============================================================================

-- Pattern 3: Investment with Company Details and Latest Valuation
-- Use this pattern for portfolio company analysis
/*
SELECT 
    inv.id as investment_id,
    f.name as fund_name,
    c.name as company_name,
    c.website,
    inv.investment_type,
    -- Primary industry
    pi.name as primary_industry,
    -- Primary country
    pc.name as primary_country,
    pc.region,
    -- Latest valuation
    is_latest.as_of_date,
    is_latest.nav,
    is_latest.cost_basis,
    is_latest.ownership_percentage
FROM {{ ref('investment') }} inv
JOIN {{ ref('fund') }} f ON inv.fund_id = f.id
JOIN {{ ref('company') }} c ON inv.company_id = c.id
-- Get primary industry
LEFT JOIN {{ ref('company_industry') }} ci ON c.id = ci.company_id AND ci.primary_flag = true
LEFT JOIN {{ ref('industry') }} pi ON ci.industry_id = pi.id
-- Get primary country
LEFT JOIN {{ ref('company_country') }} cc ON c.id = cc.company_id AND cc.primary_flag = true
LEFT JOIN {{ ref('country') }} pc ON cc.country_code = pc.code
-- Get latest investment snapshot
LEFT JOIN LATERAL (
    SELECT *
    FROM {{ ref('investment_snapshot') }} is_sub
    WHERE is_sub.investment_id = inv.id
    ORDER BY is_sub.as_of_date DESC
    LIMIT 1
) is_latest ON TRUE
ORDER BY is_latest.nav DESC NULLS LAST;
*/

-- Pattern 4: Investment with All Industry/Country Allocations
-- Use this pattern when you need complete allocation details
/*
SELECT 
    inv.id as investment_id,
    c.name as company_name,
    f.name as fund_name,
    -- Industry allocations
    STRING_AGG(i.name || ' (' || ci.allocation_pct || '%)', ', ' 
               ORDER BY ci.allocation_pct DESC) as industry_breakdown,
    -- Country allocations  
    STRING_AGG(co.name || ' (' || cc.allocation_pct || '%)', ', '
               ORDER BY cc.allocation_pct DESC) as country_breakdown
FROM {{ ref('investment') }} inv
JOIN {{ ref('fund') }} f ON inv.fund_id = f.id
JOIN {{ ref('company') }} c ON inv.company_id = c.id
-- All industry allocations
LEFT JOIN {{ ref('company_industry') }} ci ON c.id = ci.company_id
LEFT JOIN {{ ref('industry') }} i ON ci.industry_id = i.id
-- All country allocations
LEFT JOIN {{ ref('company_country') }} cc ON c.id = cc.company_id
LEFT JOIN {{ ref('country') }} co ON cc.country_code = co.code
GROUP BY inv.id, c.name, f.name
ORDER BY c.name;
*/



-- =============================================================================
-- TRANSACTION-CENTRIC JOINS
-- =============================================================================

-- Pattern 7: Transaction Flow Analysis
-- Use this pattern for cash flow and transaction analysis
/*
SELECT 
    t.id as transaction_id,
    f.name as fund_name,
    t.tx_type,
    t.transaction_date,
    t.amount,
    c.name as currency_name,
    t.description,
    -- Related entities
    CASE 
        WHEN t.commitment_id IS NOT NULL THEN inv.name
        WHEN t.investment_id IS NOT NULL THEN comp.name

        ELSE 'N/A'
    END as related_entity_name
FROM {{ ref('transaction') }} t
JOIN {{ ref('fund') }} f ON t.fund_id = f.id
JOIN {{ ref('currency') }} c ON t.currency_code = c.code
-- Optional joins based on transaction type
LEFT JOIN {{ ref('commitment') }} com ON t.commitment_id = com.id
LEFT JOIN {{ ref('investor') }} inv ON com.investor_id = inv.id
LEFT JOIN {{ ref('investment') }} i ON t.investment_id = i.id
LEFT JOIN {{ ref('company') }} comp ON i.company_id = comp.id

ORDER BY t.transaction_date DESC, f.name;
*/

-- =============================================================================
-- TIME-SERIES ANALYSIS PATTERNS
-- =============================================================================

-- Pattern 8: Fund Performance Over Time
-- Use this pattern for trend analysis and time-series reporting
/*
SELECT 
    f.id as fund_id,
    f.name as fund_name,
    f.vintage,
    fs.as_of_date,
    fs.total_nav,
    fs.dpi,
    fs.rvpi,
    -- Calculate period-over-period changes
    LAG(fs.total_nav) OVER (PARTITION BY f.id ORDER BY fs.as_of_date) as prev_nav,
    LAG(fs.dpi) OVER (PARTITION BY f.id ORDER BY fs.as_of_date) as prev_dpi,
    -- Calculate days between snapshots
    DATEDIFF('day', 
             LAG(fs.as_of_date) OVER (PARTITION BY f.id ORDER BY fs.as_of_date),
             fs.as_of_date) as days_since_last_snapshot
FROM {{ ref('fund') }} f
JOIN {{ ref('fund_snapshot') }} fs ON f.id = fs.fund_id
WHERE fs.as_of_date >= CURRENT_DATE() - INTERVAL '2 years'
ORDER BY f.vintage DESC, f.name, fs.as_of_date DESC;
*/

-- =============================================================================
-- BRIDGE TABLE PATTERNS (Many-to-Many Relationships)
-- =============================================================================

-- Pattern 9: Current Geographic/Industry Exposure for Companies
-- Use this pattern for risk analysis with company allocations
/*
SELECT 
    inv.id as investment_id,
    f.name as fund_name,
    comp.name as company_name,
    -- Current country exposure
    co.name as country_name,
    co.region,
    cc.allocation_pct as country_allocation_pct,
    -- Current industry exposure
    i.name as industry_name,
    ci.allocation_pct as industry_allocation_pct,
    -- Latest position
    is_latest.nav,
    is_latest.cost_basis
FROM {{ ref('investment') }} inv
JOIN {{ ref('fund') }} f ON inv.fund_id = f.id
JOIN {{ ref('company') }} comp ON inv.company_id = comp.id
-- Current country allocations
LEFT JOIN {{ ref('company_country') }} cc ON comp.id = cc.company_id
LEFT JOIN {{ ref('country') }} co ON cc.country_code = co.code
-- Current industry allocations
LEFT JOIN {{ ref('company_industry') }} ci ON comp.id = ci.company_id
LEFT JOIN {{ ref('industry') }} i ON ci.industry_id = i.id
-- Latest investment snapshot
LEFT JOIN LATERAL (
    SELECT nav, cost_basis
    FROM {{ ref('investment_snapshot') }} is_sub
    WHERE is_sub.investment_id = inv.id
    ORDER BY is_sub.as_of_date DESC
    LIMIT 1
) is_latest ON TRUE
ORDER BY is_latest.nav DESC NULLS LAST;
*/

-- =============================================================================
-- PERFORMANCE OPTIMIZATION NOTES
-- =============================================================================

/*
PERFORMANCE TIPS:

1. Always include fund_id in WHERE clauses when possible for RLS optimization
2. Use LATERAL joins for getting latest snapshots instead of window functions when you only need the latest record
3. Filter on as_of_date ranges to limit snapshot table scans
4. Use the clustered columns (fund_id, as_of_date) in WHERE and ORDER BY clauses
5. For bridge tables, always filter on valid_to IS NULL for current allocations
6. Consider using CTEs for complex multi-step joins to improve readability

SECURITY NOTES:

1. All these patterns respect row-level security automatically
2. Column masking will be applied based on user roles
3. Always use {{ ref() }} for table references to ensure proper dependency tracking
4. Avoid SELECT * to prevent exposure of sensitive columns
*/