-- Counterparty metrics mart
-- Contains complex relationship and transaction metrics
-- Final BI-ready model combining counterparty entities with calculated metrics

{{ config(
    materialized='table',
    tags=['bi_accessible', 'mart', 'metrics']
) }}

WITH counterparty_base AS (
    SELECT 
        id,
        name,
        counterparty_type,
        status
    FROM {{ ref('counterparty') }}
),

-- Placeholder for relationship aggregations (would join with relationship tables)
counterparty_relationships AS (
    SELECT 
        id,
        NULL as active_relationships_count,
        NULL as first_engagement_date,
        NULL as last_engagement_date
    FROM counterparty_base
),

-- Placeholder for transaction aggregations (would join with transaction tables)
counterparty_transactions AS (
    SELECT 
        id,
        NULL as total_transaction_volume
    FROM counterparty_base
)

SELECT 
    cp.id,
    cp.name,
    cp.counterparty_type,
    cp.status,
    r.active_relationships_count,
    r.first_engagement_date,
    r.last_engagement_date,
    t.total_transaction_volume,
    CURRENT_TIMESTAMP() as calculated_at
FROM counterparty_base cp
LEFT JOIN counterparty_relationships r ON cp.id = r.id
LEFT JOIN counterparty_transactions t ON cp.id = t.id