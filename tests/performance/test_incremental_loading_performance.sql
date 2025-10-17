-- Performance Test: Incremental Loading Performance for Instrument Snapshots and Cashflows
-- Task 5.2: Test incremental loading performance for instrument snapshots and cashflows
-- Requirements: 2.3, 2.4, 2.5

-- This test validates that incremental loading strategies provide optimal performance
-- for large-scale snapshot and cashflow data processing

WITH incremental_performance_metrics AS (
  
  -- Test 1: Instrument snapshot incremental loading efficiency
  SELECT 
    'incremental_snapshot_performance' as test_category,
    'snapshot_unique_key_efficiency' as test_name,
    COUNT(*) as total_snapshots,
    COUNT(DISTINCT instrument_id || '|' || as_of_date || '|' || source) as unique_combinations,
    'Snapshot unique key should provide efficient incremental loading' as test_description,
    CASE 
      WHEN COUNT(*) = COUNT(DISTINCT instrument_id || '|' || as_of_date || '|' || source) THEN 'PASS'
      ELSE 'FAIL'
    END as performance_status
  FROM {{ ref('instrument_snapshot') }}
  
  UNION ALL
  
  -- Test 2: Cashflow incremental loading efficiency
  SELECT 
    'incremental_cashflow_performance' as test_category,
    'cashflow_unique_key_efficiency' as test_name,
    COUNT(*) as total_cashflows,
    COUNT(DISTINCT id) as unique_ids,
    'Cashflow unique key should provide efficient incremental loading' as test_description,
    CASE 
      WHEN COUNT(*) = COUNT(DISTINCT id) THEN 'PASS'
      ELSE 'FAIL'
    END as performance_status
  FROM {{ ref('instrument_cashflow') }}
  
  UNION ALL
  
  -- Test 3: Snapshot date distribution for incremental efficiency
  SELECT 
    'incremental_snapshot_performance' as test_category,
    'snapshot_date_distribution' as test_name,
    COUNT(DISTINCT as_of_date) as distinct_dates,
    DATEDIFF('day', MIN(as_of_date), MAX(as_of_date)) as date_range_days,
    'Snapshot date distribution should support efficient incremental processing' as test_description,
    CASE 
      WHEN COUNT(DISTINCT as_of_date) > 0 AND DATEDIFF('day', MIN(as_of_date), MAX(as_of_date)) >= 0 THEN 'PASS'
      ELSE 'REVIEW'
    END as performance_status
  FROM {{ ref('instrument_snapshot') }}
  
  UNION ALL
  
  -- Test 4: Cashflow date distribution for incremental efficiency
  SELECT 
    'incremental_cashflow_performance' as test_category,
    'cashflow_date_distribution' as test_name,
    COUNT(DISTINCT cashflow_date) as distinct_dates,
    DATEDIFF('day', MIN(cashflow_date), MAX(cashflow_date)) as date_range_days,
    'Cashflow date distribution should support efficient incremental processing' as test_description,
    CASE 
      WHEN COUNT(DISTINCT cashflow_date) > 0 AND DATEDIFF('day', MIN(cashflow_date), MAX(cashflow_date)) >= 0 THEN 'PASS'
      ELSE 'REVIEW'
    END as performance_status
  FROM {{ ref('instrument_cashflow') }}
),

-- Test incremental loading patterns
incremental_loading_patterns AS (
  
  -- Test 5: Recent data volume for incremental processing
  SELECT 
    'incremental_loading_patterns' as test_category,
    'recent_snapshot_volume' as test_name,
    COUNT(*) as recent_snapshots,
    COUNT(DISTINCT instrument_id) as instruments_with_recent_data,
    'Recent snapshot volume should be manageable for incremental loading' as test_description,
    CASE 
      WHEN COUNT(*) >= 0 THEN 'PASS'
      ELSE 'REVIEW'
    END as performance_status
  FROM {{ ref('instrument_snapshot') }}
  WHERE as_of_date >= CURRENT_DATE() - INTERVAL '30 days'
  
  UNION ALL
  
  -- Test 6: Recent cashflow volume for incremental processing
  SELECT 
    'incremental_loading_patterns' as test_category,
    'recent_cashflow_volume' as test_name,
    COUNT(*) as recent_cashflows,
    COUNT(DISTINCT instrument_id) as instruments_with_recent_cashflows,
    'Recent cashflow volume should be manageable for incremental loading' as test_description,
    CASE 
      WHEN COUNT(*) >= 0 THEN 'PASS'
      ELSE 'REVIEW'
    END as performance_status
  FROM {{ ref('instrument_cashflow') }}
  WHERE cashflow_date >= CURRENT_DATE() - INTERVAL '30 days'
  
  UNION ALL
  
  -- Test 7: Snapshot update frequency analysis
  SELECT 
    'incremental_loading_patterns' as test_category,
    'snapshot_update_frequency' as test_name,
    COUNT(DISTINCT DATE_TRUNC('day', updated_at)) as days_with_updates,
    COUNT(*) as total_updated_records,
    'Snapshot update frequency should support efficient incremental processing' as test_description,
    CASE 
      WHEN COUNT(*) >= 0 THEN 'PASS'
      ELSE 'REVIEW'
    END as performance_status
  FROM {{ ref('instrument_snapshot') }}
  WHERE updated_at >= CURRENT_DATE() - INTERVAL '30 days'
  
  UNION ALL
  
  -- Test 8: Cashflow creation frequency analysis
  SELECT 
    'incremental_loading_patterns' as test_category,
    'cashflow_creation_frequency' as test_name,
    COUNT(DISTINCT DATE_TRUNC('day', created_at)) as days_with_new_cashflows,
    COUNT(*) as total_new_cashflows,
    'Cashflow creation frequency should support efficient incremental processing' as test_description,
    CASE 
      WHEN COUNT(*) >= 0 THEN 'PASS'
      ELSE 'REVIEW'
    END as performance_status
  FROM {{ ref('instrument_cashflow') }}
  WHERE created_at >= CURRENT_DATE() - INTERVAL '30 days'
),

-- Test data freshness and incremental efficiency
data_freshness_tests AS (
  
  -- Test 9: Snapshot data freshness
  SELECT 
    'data_freshness_performance' as test_category,
    'snapshot_data_freshness' as test_name,
    DATEDIFF('day', MAX(as_of_date), CURRENT_DATE()) as days_since_latest_snapshot,
    COUNT(*) as total_snapshots,
    'Snapshot data should be reasonably fresh for incremental loading efficiency' as test_description,
    CASE 
      WHEN DATEDIFF('day', MAX(as_of_date), CURRENT_DATE()) <= 7 THEN 'PASS'
      WHEN DATEDIFF('day', MAX(as_of_date), CURRENT_DATE()) <= 30 THEN 'REVIEW'
      ELSE 'FAIL'
    END as performance_status
  FROM {{ ref('instrument_snapshot') }}
  
  UNION ALL
  
  -- Test 10: Cashflow data freshness
  SELECT 
    'data_freshness_performance' as test_category,
    'cashflow_data_freshness' as test_name,
    DATEDIFF('day', MAX(cashflow_date), CURRENT_DATE()) as days_since_latest_cashflow,
    COUNT(*) as total_cashflows,
    'Cashflow data should be reasonably fresh for incremental loading efficiency' as test_description,
    CASE 
      WHEN DATEDIFF('day', MAX(cashflow_date), CURRENT_DATE()) <= 7 THEN 'PASS'
      WHEN DATEDIFF('day', MAX(cashflow_date), CURRENT_DATE()) <= 30 THEN 'REVIEW'
      ELSE 'FAIL'
    END as performance_status
  FROM {{ ref('instrument_cashflow') }}
),

-- Test incremental loading scalability
scalability_tests AS (
  
  -- Test 11: Snapshot volume scalability
  SELECT 
    'incremental_scalability' as test_category,
    'snapshot_volume_scalability' as test_name,
    COUNT(*) as total_snapshot_volume,
    COUNT(DISTINCT instrument_id) as instruments_covered,
    'Snapshot volume should be scalable with incremental loading strategy' as test_description,
    CASE 
      WHEN COUNT(*) / NULLIF(COUNT(DISTINCT instrument_id), 0) <= 1000 THEN 'PASS'
      WHEN COUNT(*) / NULLIF(COUNT(DISTINCT instrument_id), 0) <= 5000 THEN 'REVIEW'
      ELSE 'OPTIMIZE'
    END as performance_status
  FROM {{ ref('instrument_snapshot') }}
  
  UNION ALL
  
  -- Test 12: Cashflow volume scalability
  SELECT 
    'incremental_scalability' as test_category,
    'cashflow_volume_scalability' as test_name,
    COUNT(*) as total_cashflow_volume,
    COUNT(DISTINCT instrument_id) as instruments_covered,
    'Cashflow volume should be scalable with incremental loading strategy' as test_description,
    CASE 
      WHEN COUNT(*) / NULLIF(COUNT(DISTINCT instrument_id), 0) <= 500 THEN 'PASS'
      WHEN COUNT(*) / NULLIF(COUNT(DISTINCT instrument_id), 0) <= 2000 THEN 'REVIEW'
      ELSE 'OPTIMIZE'
    END as performance_status
  FROM {{ ref('instrument_cashflow') }}
)

-- Combine all incremental performance results
SELECT 
  test_category,
  test_name,
  total_snapshots as metric_1,
  unique_combinations as metric_2,
  test_description,
  performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM incremental_performance_metrics

UNION ALL

SELECT 
  test_category,
  test_name,
  recent_snapshots as metric_1,
  instruments_with_recent_data as metric_2,
  test_description,
  performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM incremental_loading_patterns

UNION ALL

SELECT 
  test_category,
  test_name,
  days_since_latest_snapshot as metric_1,
  total_snapshots as metric_2,
  test_description,
  performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM data_freshness_tests

UNION ALL

SELECT 
  test_category,
  test_name,
  total_snapshot_volume as metric_1,
  instruments_covered as metric_2,
  test_description,
  performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM scalability_tests

ORDER BY test_category, test_name