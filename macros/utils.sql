-- Standard dbt utility macros for common transformations
-- These are more useful than enum validation macros

{% macro generate_surrogate_key(columns) %}
  {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

{% macro safe_divide(numerator, denominator) %}
  CASE 
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL 
    THEN NULL 
    ELSE {{ numerator }} / {{ denominator }} 
  END
{% endmacro %}

{% macro calculate_days_between(start_date, end_date) %}
  DATEDIFF('day', {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro format_currency_amount(amount, currency_code) %}
  CONCAT({{ currency_code }}, ' ', TO_CHAR({{ amount }}, '999,999,999.00'))
{% endmacro %}