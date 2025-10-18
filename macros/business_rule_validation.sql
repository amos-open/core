/*
  Business Rule Validation Functions
  
  This file contains macros for validating business rules and data constraints
  across the AMOS canonical data model.
*/

-- Validate currency code format (3 characters, uppercase)
{% macro validate_currency_code(column_name) %}
  ({{ column_name }} IS NULL OR (
    LENGTH({{ column_name }}) = 3 
    AND REGEXP_CONTAINS({{ column_name }}, r'^[A-Z]{3}$')
  ))
{% endmacro %}

-- Validate country code format (2 characters, uppercase)
{% macro validate_country_code(column_name) %}
  ({{ column_name }} IS NULL OR (
    LENGTH({{ column_name }}) = 2 
    AND REGEXP_CONTAINS({{ column_name }}, r'^[A-Z]{2}$')
  ))
{% endmacro %}

-- Validate UUID format (supports multiple UUID formats)
{% macro validate_uuid_format(column_name) %}
  ({{ column_name }} IS NULL OR 
   REGEXP_CONTAINS({{ column_name }}, r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') OR
   REGEXP_CONTAINS({{ column_name }}, r'^[0-9a-fA-F]{32}$') OR
   REGEXP_CONTAINS({{ column_name }}, r'^[A-Z0-9\-]+$'))
{% endmacro %}

-- Validate positive numeric values
{% macro validate_positive_numeric(column_name) %}
  ({{ column_name }} IS NULL OR {{ column_name }} > 0)
{% endmacro %}

-- Validate non-negative numeric values
{% macro validate_non_negative_numeric(column_name) %}
  ({{ column_name }} IS NULL OR {{ column_name }} >= 0)
{% endmacro %}

-- Validate percentage values (0 to 1 as decimal)
{% macro validate_percentage(column_name) %}
  ({{ column_name }} IS NULL OR ({{ column_name }} >= 0 AND {{ column_name }} <= 1))
{% endmacro %}

-- Validate email format
{% macro validate_email_format(column_name) %}
  ({{ column_name }} IS NULL OR 
   REGEXP_CONTAINS({{ column_name }}, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'))
{% endmacro %}

-- Validate URL format
{% macro validate_url_format(column_name) %}
  ({{ column_name }} IS NULL OR 
   REGEXP_CONTAINS({{ column_name }}, r'^https?://[^\s/$.?#].[^\s]*$'))
{% endmacro %}

-- Validate vintage year (basic format validation)
{% macro validate_vintage_year(column_name) %}
  ({{ column_name }} IS NULL OR (
    {{ column_name }} > 1900 
    AND {{ column_name }} <= EXTRACT(YEAR FROM CURRENT_DATE()) + 10
  ))
{% endmacro %}

-- Validate non-empty string
{% macro validate_non_empty_string(column_name) %}
  ({{ column_name }} IS NOT NULL AND TRIM({{ column_name }}) != '')
{% endmacro %}



-- Validate counterparty type (basic non-empty validation)
{% macro validate_counterparty_type(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate investor type (basic non-empty validation)
{% macro validate_investor_type(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate fund type (basic non-empty validation)
{% macro validate_fund_type(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate data quality rating (basic non-empty validation)
{% macro validate_data_quality_rating(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate compliance status (basic non-empty validation)
{% macro validate_compliance_status(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate investment status (basic non-empty validation)
{% macro validate_investment_status(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate relationship priority (basic non-empty validation)
{% macro validate_relationship_priority(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Validate engagement strategy (basic non-empty validation)
{% macro validate_engagement_strategy(column_name) %}
  ({{ column_name }} IS NULL OR TRIM({{ column_name }}) != '')
{% endmacro %}

-- Comprehensive entity validation macro
{% macro validate_entity_base_fields(id_column, name_column) %}
  {{ validate_uuid_format(id_column) }}
  AND {{ validate_non_empty_string(name_column) }}
{% endmacro %}

-- Fund-specific validation
{% macro validate_fund_business_rules(management_fee_col, hurdle_col, carried_interest_col, target_commitment_col, base_currency_col, vintage_col) %}
  {{ validate_percentage(management_fee_col) }}
  AND {{ validate_percentage(hurdle_col) }}
  AND {{ validate_percentage(carried_interest_col) }}
  AND {{ validate_positive_numeric(target_commitment_col) }}
  AND {{ validate_currency_code(base_currency_col) }}
  AND {{ validate_vintage_year(vintage_col) }}
{% endmacro %}

-- Company-specific validation
{% macro validate_company_business_rules(currency_col, website_col, industry_id_col) %}
  {{ validate_currency_code(currency_col) }}
  AND {{ validate_url_format(website_col) }}
  AND {{ validate_uuid_format(industry_id_col) }}
{% endmacro %}

-- Counterparty-specific validation
{% macro validate_counterparty_business_rules(type_col, country_code_col) %}
  {{ validate_counterparty_type(type_col) }}
  AND {{ validate_country_code(country_code_col) }}
{% endmacro %}

-- Investor-specific validation
{% macro validate_investor_business_rules(investor_type_id_col) %}
  {{ validate_uuid_format(investor_type_id_col) }}
{% endmacro %}