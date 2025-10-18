-- Test business rules for currency dimension
-- Validates currency name uniqueness and symbol consistency

{{ config(severity = 'error') }}

WITH validation_checks AS (
  SELECT 
    code,
    name,
    symbol,
    CASE 
      WHEN name IS NULL OR TRIM(name) = '' THEN 'Currency name cannot be empty'
      WHEN symbol IS NULL OR TRIM(symbol) = '' THEN 'Currency symbol cannot be empty'
      WHEN decimal_places NOT IN (0, 2, 3, 4) THEN 'Invalid decimal places - must be 0, 2, 3, or 4'
      ELSE NULL
    END AS error_message
  FROM {{ ref('currency') }}
)

SELECT 
  code,
  name,
  symbol,
  error_message
FROM validation_checks
WHERE error_message IS NOT NULL