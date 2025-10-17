-- Instrument-Centric Entity Relationship Patterns
-- Standard patterns for joining canonical entities using the unified instrument model
-- Updated for instrument-centric design with equity and loan abstraction

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
-- INSTRUMENT-CENTRIC JOINS (UNIFIED EQUITY AND LOAN)
-- =============================================================================

-- Pattern 3: Equity Instruments with Company Details and Latest Valuation
-- Use this pattern for equity portfolio analysis
/*
SELECT 
    i.id as instrument_id,
    f.name as fund_name,
    c.name as company_name,
    c.website,
    i.instrument_type,
    i.inception_date,
    -- Primary industry (from instrument allocations)
    pi.name as primary_industry,
    -- Primary country (from instrument allocations)
    pc.name as primary_country,
    pc.region,
    -- Latest valuation
    is_latest.as_of_date,
    is_latest.fair_value_converted,
    is_latest.amortized_cost_converted,
    is_latest.equity_stake_pct,
    is_latest.equity_dividends_cum
FROM {{ ref('instrument') }} i
JOIN {{ ref('fund') }} f ON i.fund_id = f.id
JOIN {{ ref('company') }} c ON i.company_id = c.id
-- Get primary industry from instrument allocations
LEFT JOIN {{ ref('instrument_industry') }} ii ON i.id = ii.instrument_id 
    AND ii.primary_flag = true AND ii.valid_to IS NULL
LEFT JOIN {{ ref('industry') }} pi ON ii.industry_id = pi.id
-- Get primary country from instrument allocations
LEFT JOIN {{ ref('instrument_country') }} ic ON i.id = ic.instrument_id 
    AND ic.primary_flag = true AND ic.valid_to IS NULL
LEFT JOIN {{ ref('country') }} pc ON ic.country_code = pc.code
-- Get latest instrument snapshot
LEFT JOIN LATERAL (
    SELECT *
    FROM {{ ref('instrument_snapshot') }} is_sub
    WHERE is_sub.instrument_id = i.id
    ORDER BY is_sub.as_of_date DESC
    LIMIT 1
) is_latest ON TRUE
WHERE i.instrument_type = 'EQUITY'
ORDER BY is_latest.fair_value_converted DESC NULLS LAST;
*/

-- Pattern 3b: Loan Instruments with Latest Performance
-- Use this pattern for loan portfolio analysis
/*
SELECT 
    i.id as instrument_id,
    f.name as fund_name,
    i.description as loan_description,
    i.instrument_type,
    i.inception_date,
    i.termination_date,
    -- Primary country (from instrument allocations)
    pc.name as primary_country,
    pc.region,
    -- Latest loan performance
    is_latest.as_of_date,
    is_latest.fair_value_converted,
    is_latest.principal_outstanding_converted,
    is_latest.undrawn_commitment_converted,
    is_latest.accrued_income_converted
FROM {{ ref('instrument') }} i
JOIN {{ ref('fund') }} f ON i.fund_id = f.id
-- Get primary country from instrument allocations
LEFT JOIN {{ ref('instrument_country') }} ic ON i.id = ic.instrument_id 
    AND ic.primary_flag = true AND ic.valid_to IS NULL
LEFT JOIN {{ ref('country') }} pc ON ic.country_code = pc.code
-- Get latest instrument snapshot
LEFT JOIN LATERAL (
    SELECT *
    FROM {{ ref('instrument_snapshot') }} is_sub
    WHERE is_sub.instrument_id = i.id
    ORDER BY is_sub.as_of_date DESC
    LIMIT 1
) is_latest ON TRUE
WHERE i.instrument_type = 'LOAN'
ORDER BY is_latest.principal_outstanding_converted DESC NULLS LAST;
*/

-- Pattern 4: Instruments with All Industry/Country Allocations
-- Use this pattern when you need complete allocation details for any instrument type
/*
SELECT 
    i.id as instrument_id,
    i.instrument_type,
    COALESCE(c.name, i.description) as instrument_name,
    f.name as fund_name,
    -- Industry allocations (current)
    STRING_AGG(DISTINCT ind.name || ' (' || ii.allocation_pct || '%)', ', ' 
               ORDER BY ind.name || ' (' || ii.allocation_pct || '%)'
    ) FILTER (WHERE ii.valid_to IS NULL) as industry_breakdown,
    -- Country allocations (current)
    STRING_AGG(DISTINCT co.name || ' (' || ic.allocation_pct || '%)', ', '
               ORDER BY co.name || ' (' || ic.allocation_pct || '%)'
    ) FILTER (WHERE ic.valid_to IS NULL) as country_breakdown
FROM {{ ref('instrument') }} i
JOIN {{ ref('fund') }} f ON i.fund_id = f.id
LEFT JOIN {{ ref('company') }} c ON i.company_id = c.id  -- Only for equity instruments
-- All current industry allocations
LEFT JOIN {{ ref('instrument_industry') }} ii ON i.id = ii.instrument_id
LEFT JOIN {{ ref('industry') }} ind ON ii.industry_id = ind.id
-- All current country allocations
LEFT JOIN {{ ref('instrument_country') }} ic ON i.id = ic.instrument_id
LEFT JOIN {{ ref('country') }} co ON ic.country_code = co.code
GROUP BY i.id, i.instrument_type, COALESCE(c.name, i.description), f.name
ORDER BY i.instrument_type, COALESCE(c.name, i.description);
*/



-- =============================================================================
-- TRANSACTION-CENTRIC JOINS (INSTRUMENT-AWARE)
-- =============================================================================

-- Pattern 7: Transaction Flow Analysis with Instrument Context
-- Use this pattern for cash flow and transaction analysis across all instrument types
/*
SELECT 
    t.id as transaction_id,
    f.name as fund_name,
    t.transaction_type,
    t.transaction_date,
    t.amount,
    cur.name as currency_name,
    t.description,
    -- Instrument context
    i.instrument_type,
    COALESCE(comp.name, i.description) as related_entity_name,
    -- Related entities based on transaction type
    CASE 
        WHEN t.commitment_id IS NOT NULL THEN inv.name
        WHEN t.instrument_id IS NOT NULL AND i.instrument_type = 'EQUITY' THEN comp.name
        WHEN t.instrument_id IS NOT NULL AND i.instrument_type = 'LOAN' THEN i.description
        ELSE 'Fund-level transaction'
    END as transaction_context
FROM {{ ref('transaction') }} t
JOIN {{ ref('fund') }} f ON t.fund_id = f.id
JOIN {{ ref('currency') }} cur ON t.currency_code = cur.code
-- Optional joins based on transaction relationships
LEFT JOIN {{ ref('commitment') }} com ON t.commitment_id = com.id
LEFT JOIN {{ ref('investor') }} inv ON com.investor_id = inv.id
LEFT JOIN {{ ref('instrument') }} i ON t.instrument_id = i.id
LEFT JOIN {{ ref('company') }} comp ON i.company_id = comp.id
ORDER BY t.transaction_date DESC, f.name;
*/

-- Pattern 7b: Instrument Cashflow Analysis
-- Use this pattern for detailed instrument-level cashflow analysis
/*
SELECT 
    ic.id as cashflow_id,
    f.name as fund_name,
    i.instrument_type,
    COALESCE(c.name, i.description) as instrument_name,
    ic.cashflow_type,
    ic.cashflow_date,
    ic.amount,
    cur.name as currency_name,
    ic.description,
    -- Link to transaction if available
    t.transaction_type,
    t.reference_number
FROM {{ ref('instrument_cashflow') }} ic
JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
JOIN {{ ref('fund') }} f ON i.fund_id = f.id
JOIN {{ ref('currency') }} cur ON ic.currency_code = cur.code
LEFT JOIN {{ ref('company') }} c ON i.company_id = c.id  -- Only for equity
LEFT JOIN {{ ref('transaction') }} t ON ic.transaction_id = t.id
ORDER BY ic.cashflow_date DESC, f.name, i.instrument_type;
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

-- Pattern 9: Current Geographic/Industry Exposure for All Instruments
-- Use this pattern for risk analysis across all instrument types
/*
SELECT 
    i.id as instrument_id,
    i.instrument_type,
    f.name as fund_name,
    COALESCE(comp.name, i.description) as instrument_name,
    -- Current country exposure
    co.name as country_name,
    co.region,
    ic.allocation_pct as country_allocation_pct,
    ic.role as country_role,
    -- Current industry exposure
    ind.name as industry_name,
    ii.allocation_pct as industry_allocation_pct,
    -- Latest position
    is_latest.fair_value_converted,
    is_latest.amortized_cost_converted,
    -- Instrument-specific metrics
    CASE 
        WHEN i.instrument_type = 'EQUITY' THEN is_latest.equity_stake_pct
        WHEN i.instrument_type = 'LOAN' THEN is_latest.principal_outstanding_converted
        ELSE NULL
    END as type_specific_metric
FROM {{ ref('instrument') }} i
JOIN {{ ref('fund') }} f ON i.fund_id = f.id
LEFT JOIN {{ ref('company') }} comp ON i.company_id = comp.id  -- Only for equity
-- Current country allocations
LEFT JOIN {{ ref('instrument_country') }} ic ON i.id = ic.instrument_id 
    AND ic.valid_to IS NULL
LEFT JOIN {{ ref('country') }} co ON ic.country_code = co.code
-- Current industry allocations
LEFT JOIN {{ ref('instrument_industry') }} ii ON i.id = ii.instrument_id 
    AND ii.valid_to IS NULL
LEFT JOIN {{ ref('industry') }} ind ON ii.industry_id = ind.id
-- Latest instrument snapshot
LEFT JOIN LATERAL (
    SELECT *
    FROM {{ ref('instrument_snapshot') }} is_sub
    WHERE is_sub.instrument_id = i.id
    ORDER BY is_sub.as_of_date DESC
    LIMIT 1
) is_latest ON TRUE
ORDER BY is_latest.fair_value_converted DESC NULLS LAST;
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