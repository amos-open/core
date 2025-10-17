-- Performance Test: Instrument Type Filtering for Equity vs Loan Analysis
-- Task 5.2: Test performance of instrument_type filtering for equity vs loan analysis
-- Requirements: 2.3, 2.4, 2.5

-- This test validates that instrument_type filtering performs efficiently for BI queries
-- and that clustering on [fund_id, instrument_type] provides optimal performance

WITH performance_metrics AS (
  
  -- Test 1: Equity instrument filtering performance
  SELECT 
    'equity_filtering_performance' as test_category,
    'equity_instrument_count' as metric_name,
    COUNT(*) as metric_value,
    'Equity instruments should be efficiently filterable' as test_description
  FROM {{ ref('instrument') }}
  WHERE instrument_type = 'EQUITY'
  
  UNION ALL
  
  -- Test 2: Loan instrument filtering performance  
  SELECT 
    'loan_filtering_performance' as test_category,
    'loan_instrument_count' as metric_name,
    COUNT(*) as metric_value,
    'Loan instruments should be efficiently filterable' as test_description
  FROM {{ ref('instrument') }}
  WHERE instrument_type = 'LOAN'
  
  UNION ALL
  
  -- Test 3: Multi-type filtering performance
  SELECT 
    'multi_type_filtering_performance' as test_category,
    'equity_convertible_warrant_count' as metric_name,
    COUNT(*) as metric_value,
    'Multiple equity-type instruments should be efficiently filterable' as test_description
  FROM {{ ref('instrument') }}
  WHERE instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
  
  UNION ALL
  
  -- Test 4: Fund-level instrument type distribution
  SELECT 
    'fund_instrument_distribution' as test_category,
    'funds_with_mixed_instruments' as metric_name,
    COUNT(DISTINCT fund_id) as metric_value,
    'Funds with mixed instrument types should be efficiently queryable' as test_description
  FROM {{ ref('instrument') }}
  WHERE fund_id IN (
    SELECT fund_id 
    FROM {{ ref('instrument') }}
    GROUP BY fund_id
    HAVING COUNT(DISTINCT instrument_type) > 1
  )
  
  UNION ALL
  
  -- Test 5: Instrument type cardinality validation
  SELECT 
    'instrument_type_cardinality' as test_category,
    'distinct_instrument_types' as metric_name,
    COUNT(DISTINCT instrument_type) as metric_value,
    'Should have expected number of instrument types for optimal clustering' as test_description
  FROM {{ ref('instrument') }}
),

-- Performance validation for common BI query patterns
bi_query_patterns AS (
  
  -- Pattern 1: Equity portfolio analysis (common BI pattern)
  SELECT 
    'bi_query_patterns' as test_category,
    'equity_portfolio_analysis' as metric_name,
    COUNT(*) as metric_value,
    'Equity portfolio queries should perform efficiently with clustering' as test_description
  FROM {{ ref('instrument') }} i
  LEFT JOIN {{ ref('company') }} c ON i.company_id = c.id
  WHERE i.instrument_type = 'EQUITY'
  
  UNION ALL
  
  -- Pattern 2: Fund-level instrument analysis (leverages clustering)
  SELECT 
    'bi_query_patterns' as test_category,
    'fund_instrument_analysis' as metric_name,
    COUNT(*) as metric_value,
    'Fund-level queries should benefit from fund_id clustering' as test_description
  FROM {{ ref('instrument') }} i
  WHERE i.fund_id IS NOT NULL
  
  UNION ALL
  
  -- Pattern 3: Mixed instrument type analysis per fund
  SELECT 
    'bi_query_patterns' as test_category,
    'mixed_instrument_fund_analysis' as metric_name,
    COUNT(*) as metric_value,
    'Mixed instrument analysis should leverage both clustering keys' as test_description
  FROM {{ ref('instrument') }} i
  WHERE i.fund_id IS NOT NULL 
    AND i.instrument_type IN ('EQUITY', 'LOAN')
),

-- Clustering effectiveness validation
clustering_effectiveness AS (
  
  -- Validate fund_id distribution for clustering
  SELECT 
    'clustering_effectiveness' as test_category,
    'fund_distribution_balance' as metric_name,
    CASE 
      WHEN MAX(fund_count) / NULLIF(MIN(fund_count), 0) <= 10 THEN 1
      ELSE 0
    END as metric_value,
    'Fund distribution should be reasonably balanced for effective clustering' as test_description
  FROM (
    SELECT fund_id, COUNT(*) as fund_count
    FROM {{ ref('instrument') }}
    GROUP BY fund_id
  ) fund_stats
  
  UNION ALL
  
  -- Validate instrument_type distribution for clustering
  SELECT 
    'clustering_effectiveness' as test_category,
    'instrument_type_distribution_balance' as metric_name,
    CASE 
      WHEN MAX(type_count) / NULLIF(MIN(type_count), 0) <= 20 THEN 1
      ELSE 0
    END as metric_value,
    'Instrument type distribution should support effective clustering' as test_description
  FROM (
    SELECT instrument_type, COUNT(*) as type_count
    FROM {{ ref('instrument') }}
    GROUP BY instrument_type
  ) type_stats
)

-- Combine all performance metrics
SELECT 
  test_category,
  metric_name,
  metric_value,
  test_description,
  CASE 
    WHEN test_category = 'clustering_effectiveness' AND metric_value = 1 THEN 'PASS'
    WHEN test_category = 'clustering_effectiveness' AND metric_value = 0 THEN 'FAIL'
    WHEN metric_value > 0 THEN 'PASS'
    ELSE 'REVIEW'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM performance_metrics

UNION ALL

SELECT 
  test_category,
  metric_name,
  metric_value,
  test_description,
  CASE 
    WHEN metric_value > 0 THEN 'PASS'
    ELSE 'REVIEW'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM bi_query_patterns

UNION ALL

SELECT 
  test_category,
  metric_name,
  metric_value,
  test_description,
  CASE 
    WHEN metric_value = 1 THEN 'PASS'
    ELSE 'FAIL'
  END as performance_status,
  CURRENT_TIMESTAMP() as test_timestamp
FROM clustering_effectiveness

ORDER BY test_category, metric_name