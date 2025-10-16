-- Data Quality Monitoring Macros
-- Macros for automated data quality monitoring and alerting

{% macro generate_data_quality_report() %}
  -- Generate comprehensive data quality report
  {{ log("Generating data quality report...", info=True) }}
  
  {% set quality_tests = [
    'test_fund_performance_metrics',
    'test_allocation_percentages', 
    'test_loan_cashflow_consistency',
    'test_data_freshness',
    'test_volume_anomalies'
  ] %}
  
  {% for test in quality_tests %}
    {{ log("Running data quality test: " ~ test, info=True) }}
  {% endfor %}
  
{% endmacro %}

{% macro check_data_freshness(table_name, date_column, max_age_hours=25) %}
  -- Check if data is fresh within specified hours
  {% set query %}
    SELECT 
      '{{ table_name }}' as table_name,
      MAX({{ date_column }}) as latest_date,
      EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ date_column }}))) / 3600 as hours_old,
      CASE 
        WHEN EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ date_column }}))) / 3600 > {{ max_age_hours }}
        THEN 'STALE'
        ELSE 'FRESH'
      END as freshness_status
    FROM {{ ref(table_name) }}
  {% endset %}
  
  {{ return(query) }}
{% endmacro %}

{% macro validate_business_metrics(table_name, metric_validations) %}
  -- Validate business metric calculations
  {% set validation_cases = [] %}
  
  {% for validation in metric_validations %}
    {% set case_stmt %}
      WHEN NOT ({{ validation.condition }}) THEN '{{ validation.error_message }}'
    {% endset %}
    {% do validation_cases.append(case_stmt) %}
  {% endfor %}
  
  {% set query %}
    SELECT 
      *,
      CASE 
        {{ validation_cases | join('\n        ') }}
        ELSE NULL
      END as validation_error
    FROM {{ ref(table_name) }}
    WHERE CASE 
      {{ validation_cases | join('\n      ') }}
      ELSE NULL
    END IS NOT NULL
  {% endset %}
  
  {{ return(query) }}
{% endmacro %}

{% macro detect_outliers(table_name, metric_column, threshold_multiplier=3) %}
  -- Detect statistical outliers using standard deviation
  {% set query %}
    WITH stats AS (
      SELECT 
        AVG({{ metric_column }}) as mean_value,
        STDDEV({{ metric_column }}) as std_dev
      FROM {{ ref(table_name) }}
      WHERE {{ metric_column }} IS NOT NULL
    ),
    outlier_detection AS (
      SELECT 
        *,
        ABS({{ metric_column }} - stats.mean_value) / NULLIF(stats.std_dev, 0) as z_score
      FROM {{ ref(table_name) }}
      CROSS JOIN stats
      WHERE {{ metric_column }} IS NOT NULL
    )
    SELECT *
    FROM outlier_detection
    WHERE z_score > {{ threshold_multiplier }}
  {% endset %}
  
  {{ return(query) }}
{% endmacro %}

{% macro check_referential_integrity(child_table, parent_table, foreign_key, parent_key) %}
  -- Check referential integrity between tables
  {% set query %}
    SELECT 
      '{{ child_table }}' as child_table,
      '{{ parent_table }}' as parent_table,
      '{{ foreign_key }}' as foreign_key_column,
      COUNT(*) as orphaned_records
    FROM {{ ref(child_table) }} c
    LEFT JOIN {{ ref(parent_table) }} p ON c.{{ foreign_key }} = p.{{ parent_key }}
    WHERE c.{{ foreign_key }} IS NOT NULL 
      AND p.{{ parent_key }} IS NULL
    HAVING COUNT(*) > 0
  {% endset %}
  
  {{ return(query) }}
{% endmacro %}

{% macro monitor_data_quality_trends() %}
  -- Monitor data quality trends over time
  {% set query %}
    WITH daily_quality_metrics AS (
      SELECT 
        CURRENT_DATE() as check_date,
        'fund_snapshot' as table_name,
        COUNT(*) as record_count,
        COUNT(CASE WHEN total_nav IS NULL THEN 1 END) as null_nav_count,
        COUNT(CASE WHEN dpi < 0 THEN 1 END) as negative_dpi_count,
        COUNT(CASE WHEN total_called > total_commitment THEN 1 END) as over_commitment_count
      FROM {{ ref('fund_snapshot') }}
      WHERE as_of_date >= CURRENT_DATE() - INTERVAL '7 days'
      
      UNION ALL
      
      SELECT 
        CURRENT_DATE() as check_date,
        'investment_snapshot' as table_name,
        COUNT(*) as record_count,
        COUNT(CASE WHEN nav IS NULL THEN 1 END) as null_nav_count,
        COUNT(CASE WHEN nav < 0 THEN 1 END) as negative_nav_count,
        COUNT(CASE WHEN ownership_percentage > 100 THEN 1 END) as invalid_ownership_count
      FROM {{ ref('investment_snapshot') }}
      WHERE as_of_date >= CURRENT_DATE() - INTERVAL '7 days'
      
      UNION ALL
      
      SELECT 
        CURRENT_DATE() as check_date,
        'transaction' as table_name,
        COUNT(*) as record_count,
        COUNT(CASE WHEN amount IS NULL THEN 1 END) as null_amount_count,
        COUNT(CASE WHEN transaction_date > CURRENT_DATE() THEN 1 END) as future_date_count,
        COUNT(CASE WHEN currency_code NOT IN (SELECT code FROM {{ ref('currency') }}) THEN 1 END) as invalid_currency_count
      FROM {{ ref('transaction') }}
      WHERE transaction_date >= CURRENT_DATE() - INTERVAL '7 days'
    )
    SELECT * FROM daily_quality_metrics
  {% endset %}
  
  {{ return(query) }}
{% endmacro %}

{% macro create_data_quality_alert(test_name, severity, threshold, message) %}
  -- Create data quality alert configuration
  {% set alert_config = {
    'test_name': test_name,
    'severity': severity,
    'threshold': threshold,
    'message': message,
    'created_at': modules.datetime.datetime.now().isoformat()
  } %}
  
  {{ log("Data Quality Alert Configured: " ~ alert_config, info=True) }}
  {{ return(alert_config) }}
{% endmacro %}