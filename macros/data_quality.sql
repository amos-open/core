{% macro test_not_empty_string(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL 
    AND TRIM({{ column_name }}) = ''
{% endmacro %}

{% macro test_valid_uuid(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND NOT REGEXP_LIKE({{ column_name }}, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
{% endmacro %}

{% macro test_positive_amount(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND {{ column_name }} <= 0
{% endmacro %}

{% macro test_valid_currency_code(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND NOT REGEXP_LIKE({{ column_name }}, '^[A-Z]{3}$')
{% endmacro %}

{% macro test_valid_country_code(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND NOT REGEXP_LIKE({{ column_name }}, '^[A-Z]{2}$')
{% endmacro %}