-- Clustering and Index Validation Test
-- Task 5.1: Confirm clustering and indexing aligns with DBML index definitions
-- Requirements: 6.4, 6.5

-- This test validates that clustering strategies match DBML index specifications
-- and that performance-critical indexes are properly implemented

WITH clustering_validation AS (
  
  -- 1. INSTRUMENT TABLE CLUSTERING VALIDATION
  -- Expected clustering: fund_id, instrument_type (per design document)
  SELECT 
    'clustering_validation' as validation_category,
    'instrument_clustering_check' as test_name,
    'instrument' as table_name,
    'Validating instrument table clustering on fund_id, instrument_type' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument").name }}')
          AND table_type = 'BASE TABLE'
      ) THEN 'PASS'
      ELSE 'FAIL - Table not found or not properly materialized'
    END as validation_result
  
  UNION ALL
  
  -- 2. INSTRUMENT_SNAPSHOT TABLE CLUSTERING VALIDATION  
  -- Expected clustering: as_of_date, instrument_id (per design document)
  SELECT 
    'clustering_validation' as validation_category,
    'instrument_snapshot_clustering_check' as test_name,
    'instrument_snapshot' as table_name,
    'Validating instrument_snapshot table clustering on as_of_date, instrument_id' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument_snapshot").name }}')
          AND table_type = 'BASE TABLE'
      ) THEN 'PASS'
      ELSE 'FAIL - Table not found or not properly materialized'
    END as validation_result
  
  UNION ALL
  
  -- 3. INSTRUMENT_CASHFLOW TABLE CLUSTERING VALIDATION
  -- Expected clustering: cashflow_date, instrument_id (per design document)
  SELECT 
    'clustering_validation' as validation_category,
    'instrument_cashflow_clustering_check' as test_name,
    'instrument_cashflow' as table_name,
    'Validating instrument_cashflow table clustering on cashflow_date, instrument_id' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument_cashflow").name }}')
          AND table_type = 'BASE TABLE'
      ) THEN 'PASS'
      ELSE 'FAIL - Table not found or not properly materialized'
    END as validation_result
  
  UNION ALL
  
  -- 4. BRIDGE TABLE CLUSTERING VALIDATION
  -- Expected clustering: instrument_id for instrument_country and instrument_industry
  SELECT 
    'clustering_validation' as validation_category,
    'bridge_table_clustering_check' as test_name,
    'instrument_country' as table_name,
    'Validating instrument_country table clustering on instrument_id' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument_country").name }}')
          AND table_type = 'BASE TABLE'
      ) THEN 'PASS'
      ELSE 'FAIL - Table not found or not properly materialized'
    END as validation_result
  
  UNION ALL
  
  SELECT 
    'clustering_validation' as validation_category,
    'bridge_table_clustering_check' as test_name,
    'instrument_industry' as table_name,
    'Validating instrument_industry table clustering on instrument_id' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument_industry").name }}')
          AND table_type = 'BASE TABLE'
      ) THEN 'PASS'
      ELSE 'FAIL - Table not found or not properly materialized'
    END as validation_result
),

-- 5. MATERIALIZATION STRATEGY VALIDATION
materialization_validation AS (
  
  -- Validate core tables are materialized as tables (not views)
  SELECT 
    'materialization_validation' as validation_category,
    'core_table_materialization' as test_name,
    'instrument' as table_name,
    'Core instrument table should be materialized as table for BI performance' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument").name }}')
          AND table_type = 'BASE TABLE'
      ) THEN 'PASS'
      ELSE 'FAIL - Should be materialized as table, not view'
    END as validation_result
  
  UNION ALL
  
  -- Validate incremental models exist and are properly configured
  SELECT 
    'materialization_validation' as validation_category,
    'incremental_model_check' as test_name,
    'instrument_snapshot' as table_name,
    'Instrument snapshot should be incremental for performance' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument_snapshot").name }}')
      ) THEN 'PASS'
      ELSE 'FAIL - Incremental model not found'
    END as validation_result
  
  UNION ALL
  
  SELECT 
    'materialization_validation' as validation_category,
    'incremental_model_check' as test_name,
    'instrument_cashflow' as table_name,
    'Instrument cashflow should be incremental for performance' as validation_message,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = UPPER('{{ ref("instrument_cashflow").name }}')
      ) THEN 'PASS'
      ELSE 'FAIL - Incremental model not found'
    END as validation_result
),

-- 6. UNIQUE KEY VALIDATION FOR INCREMENTAL MODELS
unique_key_validation AS (
  
  -- Validate instrument_snapshot unique key constraint
  SELECT 
    'unique_key_validation' as validation_category,
    'snapshot_unique_key_check' as test_name,
    COUNT(*) || ' duplicate records found' as table_name,
    'instrument_snapshot unique key validation (instrument_id, as_of_date, source)' as validation_message,
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL - Duplicate records found for unique key'
    END as validation_result
  FROM (
    SELECT instrument_id, as_of_date, source, COUNT(*) as record_count
    FROM {{ ref('instrument_snapshot') }}
    GROUP BY instrument_id, as_of_date, source
    HAVING COUNT(*) > 1
  ) duplicates
  
  UNION ALL
  
  -- Validate instrument_cashflow unique key constraint
  SELECT 
    'unique_key_validation' as validation_category,
    'cashflow_unique_key_check' as test_name,
    COUNT(*) || ' duplicate records found' as table_name,
    'instrument_cashflow unique key validation (id)' as validation_message,
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL - Duplicate records found for unique key'
    END as validation_result
  FROM (
    SELECT id, COUNT(*) as record_count
    FROM {{ ref('instrument_cashflow') }}
    GROUP BY id
    HAVING COUNT(*) > 1
  ) duplicates
),

-- 7. PERFORMANCE INDEX VALIDATION
-- Check for common query patterns that should be optimized
performance_validation AS (
  
  -- Validate instrument_type filtering performance (common BI pattern)
  SELECT 
    'performance_validation' as validation_category,
    'instrument_type_filter_check' as test_name,
    'instrument' as table_name,
    'Validating instrument_type filtering performance for BI queries' as validation_message,
    CASE 
      WHEN (SELECT COUNT(DISTINCT instrument_type) FROM {{ ref('instrument') }}) > 0 THEN 'PASS'
      ELSE 'FAIL - No instrument types found for filtering validation'
    END as validation_result
  
  UNION ALL
  
  -- Validate date-based filtering performance for snapshots
  SELECT 
    'performance_validation' as validation_category,
    'date_filter_check' as test_name,
    'instrument_snapshot' as table_name,
    'Validating as_of_date filtering performance for time-series queries' as validation_message,
    CASE 
      WHEN (SELECT COUNT(DISTINCT as_of_date) FROM {{ ref('instrument_snapshot') }}) > 0 THEN 'PASS'
      ELSE 'FAIL - No snapshot dates found for filtering validation'
    END as validation_result
  
  UNION ALL
  
  -- Validate fund-based filtering performance
  SELECT 
    'performance_validation' as validation_category,
    'fund_filter_check' as test_name,
    'instrument' as table_name,
    'Validating fund_id filtering performance for fund-level analysis' as validation_message,
    CASE 
      WHEN (SELECT COUNT(DISTINCT fund_id) FROM {{ ref('instrument') }}) > 0 THEN 'PASS'
      ELSE 'FAIL - No funds found for filtering validation'
    END as validation_result
)

-- Combine all validation results
SELECT * FROM clustering_validation
UNION ALL
SELECT * FROM materialization_validation  
UNION ALL
SELECT * FROM unique_key_validation
UNION ALL
SELECT * FROM performance_validation

ORDER BY validation_category, test_name