-- Data Quality Test: Data Freshness Monitoring
-- Tests that critical tables have been updated within expected timeframes

{{ config(
    severity='warn',
    tags=['data_quality', 'freshness', 'monitoring']
) }}

-- Test data freshness for time-sensitive tables
WITH freshness_check AS (
    SELECT 
        'fund_snapshot' as table_name,
        MAX(as_of_date) as latest_date,
        MAX(updated_at) as latest_update,
        COUNT(*) as total_records,
        COUNT(DISTINCT fund_id) as unique_entities,
        CURRENT_DATE() - MAX(as_of_date) as days_since_latest_snapshot,
        CURRENT_TIMESTAMP() - MAX(updated_at) as hours_since_latest_update,
        CASE 
            WHEN CURRENT_DATE() - MAX(as_of_date) > 2 THEN 'Fund snapshots are stale (>2 days old)'
            WHEN CURRENT_TIMESTAMP() - MAX(updated_at) > INTERVAL '25 hours' THEN 'Fund snapshots not updated in last 25 hours'
            ELSE NULL
        END as freshness_issue
    FROM {{ ref('fund_snapshot') }}
    
    UNION ALL
    
    SELECT 
        'investment_snapshot' as table_name,
        MAX(as_of_date) as latest_date,
        MAX(updated_at) as latest_update,
        COUNT(*) as total_records,
        COUNT(DISTINCT investment_id) as unique_entities,
        CURRENT_DATE() - MAX(as_of_date) as days_since_latest_snapshot,
        CURRENT_TIMESTAMP() - MAX(updated_at) as hours_since_latest_update,
        CASE 
            WHEN CURRENT_DATE() - MAX(as_of_date) > 2 THEN 'Investment snapshots are stale (>2 days old)'
            WHEN CURRENT_TIMESTAMP() - MAX(updated_at) > INTERVAL '25 hours' THEN 'Investment snapshots not updated in last 25 hours'
            ELSE NULL
        END as freshness_issue
    FROM {{ ref('investment_snapshot') }}
    

    
    UNION ALL
    
    SELECT 
        'transaction' as table_name,
        MAX(transaction_date) as latest_date,
        MAX(updated_at) as latest_update,
        COUNT(*) as total_records,
        COUNT(DISTINCT fund_id) as unique_entities,
        CURRENT_DATE() - MAX(transaction_date) as days_since_latest_snapshot,
        CURRENT_TIMESTAMP() - MAX(updated_at) as hours_since_latest_update,
        CASE 
            WHEN CURRENT_TIMESTAMP() - MAX(updated_at) > INTERVAL '6 hours' THEN 'Transactions not updated in last 6 hours'
            ELSE NULL
        END as freshness_issue
    FROM {{ ref('transaction') }}
    WHERE transaction_date >= CURRENT_DATE() - INTERVAL '7 days'  -- Only check recent transactions
    

)

SELECT 
    table_name,
    latest_date,
    latest_update,
    total_records,
    unique_entities,
    days_since_latest_snapshot,
    EXTRACT(EPOCH FROM hours_since_latest_update) / 3600 as hours_since_latest_update,
    freshness_issue
FROM freshness_check
WHERE freshness_issue IS NOT NULL
ORDER BY table_name