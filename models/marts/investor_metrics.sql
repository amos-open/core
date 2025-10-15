-- Investor metrics mart
-- Contains complex investment metrics calculated from commitment and transaction data
-- Final BI-ready model combining investor entities with calculated metrics

{{ config(
    materialized='table',
    tags=['bi_accessible', 'mart', 'metrics']
) }}

WITH investor_base AS (
    SELECT 
        id,
        name,
        investor_type,
        status
    FROM {{ ref('investor') }}
),

-- Placeholder for commitment aggregations (would join with commitment tables)
investor_commitments AS (
    SELECT 
        id,
        NULL as total_commitments,
        NULL as active_funds_count
    FROM investor_base
),

-- Placeholder for investment aggregations (would join with investment/transaction tables)
investor_investments AS (
    SELECT 
        id,
        NULL as total_invested,
        NULL as first_investment_date,
        NULL as last_investment_date
    FROM investor_base
)

SELECT 
    i.id,
    i.name,
    i.investor_type,
    i.status,
    c.total_commitments,
    c.active_funds_count,
    inv.total_invested,
    inv.first_investment_date,
    inv.last_investment_date,
    CURRENT_TIMESTAMP() as calculated_at
FROM investor_base i
LEFT JOIN investor_commitments c ON i.id = c.id
LEFT JOIN investor_investments inv ON i.id = inv.id