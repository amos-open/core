-- Test to ensure all country codes in dim_country follow ISO 3166 standard
-- This test validates that country codes are exactly 2 uppercase letters

{{ config(severity = 'error') }}

SELECT 
  code,
  iso3_code,
  name,
  'Invalid ISO country code format' AS error_message
FROM {{ ref('dim_country') }}
WHERE 
  -- Check 2-letter code format
  (code IS NULL OR LENGTH(code) != 2 OR NOT REGEXP_LIKE(code, '^[A-Z]{2}$'))
  OR
  -- Check 3-letter code format  
  (iso3_code IS NULL OR LENGTH(iso3_code) != 3 OR NOT REGEXP_LIKE(iso3_code, '^[A-Z]{3}$'))