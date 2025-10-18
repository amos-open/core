-- Test to ensure all currency codes in dim_currency follow ISO 4217 standard
-- This test validates that currency codes are exactly 3 uppercase letters

{{ config(severity = 'error') }}

SELECT 
  code,
  'Invalid ISO currency code format' AS error_message
FROM {{ ref('currency') }}
WHERE 
  code IS NULL 
  OR LENGTH(code) != 3
  OR NOT REGEXP_LIKE(code, '^[A-Z]{3}$')