-- Data Quality Test: Volume Anomaly Detection
-- Tests for unusual data patterns and volume changes

{{ config(
    severity='warn',
    tags=['data_quality', 'volume_anomaly', 'monitoring']
) }}

-- Test for volume anomalies in key tables
WITH volume_analysis AS (
    -- Daily transaction volume analysis
    SELECT 
        'transaction_volume' as anomaly_type,
        DATE(transaction_date) as analysis_date,
        COUNT(*) as daily_count,
        SUM(ABS(amount)) as daily_volume,
        AVG(COUNT(*)) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as avg_count_7day,
        AVG(SUM(ABS(amount))) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as avg_volume_7day,
        CASE 
            WHEN COUNT(*) > 2 * AVG(COUNT(*)) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) 
                THEN 'Transaction count spike: ' || COUNT(*) || ' vs 7-day avg ' || ROUND(AVG(COUNT(*)) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            WHEN COUNT(*) < 0.3 * AVG(COUNT(*)) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AND AVG(COUNT(*)) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) > 5
                THEN 'Transaction count drop: ' || COUNT(*) || ' vs 7-day avg ' || ROUND(AVG(COUNT(*)) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            WHEN SUM(ABS(amount)) > 3 * AVG(SUM(ABS(amount))) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) 
                THEN 'Transaction volume spike: ' || ROUND(SUM(ABS(amount)), 0) || ' vs 7-day avg ' || ROUND(AVG(SUM(ABS(amount))) OVER (ORDER BY DATE(transaction_date) ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            ELSE NULL
        END as anomaly_description
    FROM {{ ref('transaction') }}
    WHERE transaction_date >= CURRENT_DATE() - INTERVAL '30 days'
    GROUP BY DATE(transaction_date)
    
    UNION ALL
    
    -- Fund snapshot record count analysis
    SELECT 
        'fund_snapshot_volume' as anomaly_type,
        as_of_date as analysis_date,
        COUNT(*) as daily_count,
        COUNT(DISTINCT fund_id) as daily_volume,
        AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as avg_count_7day,
        AVG(COUNT(DISTINCT fund_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as avg_volume_7day,
        CASE 
            WHEN COUNT(*) < 0.5 * AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AND AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) > 10
                THEN 'Fund snapshot count drop: ' || COUNT(*) || ' vs 7-day avg ' || ROUND(AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            WHEN COUNT(DISTINCT fund_id) < 0.7 * AVG(COUNT(DISTINCT fund_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AND AVG(COUNT(DISTINCT fund_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) > 5
                THEN 'Fund coverage drop: ' || COUNT(DISTINCT fund_id) || ' funds vs 7-day avg ' || ROUND(AVG(COUNT(DISTINCT fund_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            ELSE NULL
        END as anomaly_description
    FROM {{ ref('fund_snapshot') }}
    WHERE as_of_date >= CURRENT_DATE() - INTERVAL '30 days'
    GROUP BY as_of_date
    
    UNION ALL
    
    -- Instrument snapshot record count analysis
    SELECT 
        'instrument_snapshot_volume' as anomaly_type,
        as_of_date as analysis_date,
        COUNT(*) as daily_count,
        COUNT(DISTINCT instrument_id) as daily_volume,
        AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as avg_count_7day,
        AVG(COUNT(DISTINCT instrument_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as avg_volume_7day,
        CASE 
            WHEN COUNT(*) < 0.5 * AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AND AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) > 10
                THEN 'Instrument snapshot count drop: ' || COUNT(*) || ' vs 7-day avg ' || ROUND(AVG(COUNT(*)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            WHEN COUNT(DISTINCT instrument_id) < 0.7 * AVG(COUNT(DISTINCT instrument_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AND AVG(COUNT(DISTINCT instrument_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) > 5
                THEN 'Instrument coverage drop: ' || COUNT(DISTINCT instrument_id) || ' instruments vs 7-day avg ' || ROUND(AVG(COUNT(DISTINCT instrument_id)) OVER (ORDER BY as_of_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 0)
            ELSE NULL
        END as anomaly_description
    FROM {{ ref('instrument_snapshot') }}
    WHERE as_of_date >= CURRENT_DATE() - INTERVAL '30 days'
    GROUP BY as_of_date
    

)

SELECT 
    anomaly_type,
    analysis_date,
    daily_count,
    daily_volume,
    ROUND(avg_count_7day, 1) as avg_count_7day,
    ROUND(avg_volume_7day, 0) as avg_volume_7day,
    anomaly_description
FROM volume_analysis
WHERE anomaly_description IS NOT NULL
  AND analysis_date >= CURRENT_DATE() - INTERVAL '14 days'  -- Focus on recent anomalies
ORDER BY analysis_date DESC, anomaly_type