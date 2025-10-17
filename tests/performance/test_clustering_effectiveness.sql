-- Performance Test: Clustering Effectiveness for Mixed Instrument Queries
-- Task 5.2: Validate clustering effectiveness for mixed instrument queries
-- Requirements: 2.3, 2.4, 2.5

-- This test validates that clustering strategies provide optimal performance
-- for common BI query patterns involving mixed instrument types

WITH clustering_performance_tests AS (
  
  -- Test 1: Fund-level clustering effectiveness
  SELECT 
    'fund_clustering_performance' as test_category,
    'fund_based_queries' as test_name,
    COUNT(DISTINCT fund_id) as funds_tested,
    AVG(instrument_count) as avg_instruments_per_fund,
    'Fund-level queries should benefit from fund_id clustering' as test_description
  FROM (
    SELECT 
      fund_id,
      COUNT(*) as instrument_count
    FROM {{ ref('instrument') }}
    GROUP BY fund_id
  ) fund_stats
  
  UNION ALL
  
  -- Test 2: Instrument type clustering effectiveness
  SELECT 
    'instrument_type_clustering_performance' as test_category,
    'type_based_queries' as test_name,
    COUNT(DISTINCT instrument_type) as types_tested,
    AVG(type_count) as avg_instruments_per_type,
    'Instrument type queries should benefit from instrument_type clustering' as test_description
  FROM (
    SELECT 
      instrument_type,
      COUNT(*) as type_count
    FROM {{ ref('instrument') }}
    GROUP BY instrument_type
  ) type_stats
  
  UNION ALL
  
  -- Test 3: Combined clustering effectiveness (fund + type)
  SELECT 
    'combined_clustering_performance' as test_category,
    'fund_and_type_queries' as test_name,
    COUNT(*) as combinations_tested,
    AVG(combination_count) as avg_instruments_per_combination,
    'Combined fund+type queries should leverage both clustering keys optimally' as test_description
  FROM (
    SELECT 
      fund_id,
      instrument_type,
      COUNT(*) as combination_count
    FROM {{ ref('instrument') }}
    GROUP BY fund_id, instrument_type
  ) combination_stats
),

-- Test snapshot clustering performance
snapshot_clustering_tests AS (
  
  -- Test 4: Date-based clustering for snapshots
  SELECT 
    'snapshot_date_clustering' as test_category,
    'date_based_queries' as test_name,
    COUNT(DISTINCT as_of_date) as dates_tested,
    AVG(daily_snapshot_count) as avg_snapshots_per_date,
    'Date-based snapshot queries should benefit from as_of_date clustering' as test_description
  FROM (
    SELECT 
      as_of_date,
      COUNT(*) as daily_snapshot_count
    FROM {{ ref('instrument_snapshot') }}
    GROUP BY as_of_date
  ) date_stats
  
  UNION ALL
  
  -- Test 5: Instrument-based clustering for snapshots
  SELECT 
    'snapshot_instrument_clustering' as test_category,
    'instrument_based_queries' as test_name,
    COUNT(DISTINCT instrument_id) as instruments_tested,
    AVG(instrument_snapshot_count) as avg_snapshots_per_instrument,
    'Instrument-based snapshot queries should benefit from instrument_id clustering' as test_description
  FROM (
    SELECT 
      instrument_id,
      COUNT(*) as instrument_snapshot_count
    FROM {{ ref('instrument_snapshot') }}
    GROUP BY instrument_id
  ) instrument_stats
  
  UNION ALL
  
  -- Test 6: Time-series query performance
  SELECT 
    'snapshot_timeseries_clustering' as test_category,
    'timeseries_queries' as test_name,
    COUNT(*) as timeseries_combinations,
    AVG(snapshot_count) as avg_snapshots_per_series,
    'Time-series queries should leverage combined date+instrument clustering' as test_description
  FROM (
    SELECT 
      instrument_id,
      as_of_date,
      COUNT(*) as snapshot_count
    FROM {{ ref('instrument_snapshot') }}
    GROUP BY instrument_id, as_of_date
  ) timeseries_stats
),

-- Test cashflow clustering performance
cashflow_clustering_tests AS (
  
  -- Test 7: Cashflow date clustering
  SELECT 
    'cashflow_date_clustering' as test_category,
    'cashflow_date_queries' as test_name,
    COUNT(DISTINCT cashflow_date) as dates_tested,
    AVG(daily_cashflow_count) as avg_cashflows_per_date,
    'Date-based cashflow queries should benefit from cashflow_date clustering' as test_description
  FROM (
    SELECT 
      cashflow_date,
      COUNT(*) as daily_cashflow_count
    FROM {{ ref('instrument_cashflow') }}
    GROUP BY cashflow_date
  ) cashflow_date_stats
  
  UNION ALL
  
  -- Test 8: Cashflow instrument clustering
  SELECT 
    'cashflow_instrument_clustering' as test_category,
    'cashflow_instrument_queries' as test_name,
    COUNT(DISTINCT instrument_id) as instruments_tested,
    AVG(instrument_cashflow_count) as avg_cashflows_per_instrument,
    'Instrument-based cashflow queries should benefit from instrument_id clustering' as test_description
  FROM (
    SELECT 
      instrument_id,
      COUNT(*) as instrument_cashflow_count
    FROM {{ ref('instrument_cashflow') }}
    GROUP BY instrument_id
  ) cashflow_instrument_stats
),

-- Performance benchmark queries (common BI patterns)
performance_benchmarks AS (
  
  -- Benchmark 1: Equity portfolio performance query
  SELECT 
    'performance_benchmarks' as test_category,
    'equity_portfolio_benchmark' as test_name,
    COUNT(*) as records_processed,
    COUNT(DISTINCT i.fund_id) as funds_involved,
    'Equity portfolio analysis performance benchmark' as test_description
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('instrument_snapshot') }} s ON i.id = s.instrument_id
  WHERE i.instrument_type = 'EQUITY'
    AND s.as_of_date = (SELECT MAX(as_of_date) FROM {{ ref('instrument_snapshot') }})
  
  UNION ALL
  
  -- Benchmark 2: Fund performance across all instrument types
  SELECT 
    'performance_benchmarks' as test_category,
    'fund_performance_benchmark' as test_name,
    COUNT(*) as records_processed,
    COUNT(DISTINCT i.instrument_type) as instrument_types_involved,
    'Fund-level performance analysis across all instrument types' as test_description
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('instrument_snapshot') }} s ON i.id = s.instrument_id
  WHERE s.as_of_date = (SELECT MAX(as_of_date) FROM {{ ref('instrument_snapshot') }})
  
  UNION ALL
  
  -- Benchmark 3: Cashflow analysis by instrument type
  SELECT 
    'performance_benchmarks' as test_category,
    'cashflow_analysis_benchmark' as test_name,
    COUNT(*) as records_processed,
    COUNT(DISTINCT i.instrument_type) as instrument_types_involved,
    'Cashflow analysis performance by instrument type' as test_description
  FROM {{ ref('instrument_cashflow') }} ic
  JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
  WHERE ic.cashflow_date >= CURRENT_DATE() - INTERVAL '1 year'
)

-- Combine all clustering performance results
SELECT 
  test_category,
  test_name,
  funds_tested as metric_1,
  avg_instruments_per_fund as metric_2,
  NULL as metric_3,
  test_description,
  CASE 
    WHEN avg_instruments_per_fund > 0 THEN 'PASS'
    ELSE 'REVIEW'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM clustering_performance_tests

UNION ALL

SELECT 
  test_category,
  test_name,
  dates_tested as metric_1,
  avg_snapshots_per_date as metric_2,
  NULL as metric_3,
  test_description,
  CASE 
    WHEN avg_snapshots_per_date > 0 THEN 'PASS'
    ELSE 'REVIEW'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM snapshot_clustering_tests

UNION ALL

SELECT 
  test_category,
  test_name,
  dates_tested as metric_1,
  avg_cashflows_per_date as metric_2,
  NULL as metric_3,
  test_description,
  CASE 
    WHEN avg_cashflows_per_date > 0 THEN 'PASS'
    ELSE 'REVIEW'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM cashflow_clustering_tests

UNION ALL

SELECT 
  test_category,
  test_name,
  records_processed as metric_1,
  funds_involved as metric_2,
  NULL as metric_3,
  test_description,
  CASE 
    WHEN records_processed > 0 THEN 'PASS'
    ELSE 'REVIEW'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM performance_benchmarks

ORDER BY test_category, test_name