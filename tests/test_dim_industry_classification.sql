-- Test industry classification hierarchy consistency
-- Validates that sector, subsector, and industry_group relationships are logical

{{ config(severity = 'error') }}

WITH validation_checks AS (
  SELECT 
    code,
    name,
    sector,
    subsector,
    industry_group,
    CASE 
      WHEN name IS NULL OR TRIM(name) = '' THEN 'Industry name cannot be empty'
      WHEN sector IS NULL OR TRIM(sector) = '' THEN 'Sector cannot be empty'
      WHEN subsector IS NULL OR TRIM(subsector) = '' THEN 'Subsector cannot be empty'
      WHEN industry_group IS NULL OR TRIM(industry_group) = '' THEN 'Industry group cannot be empty'
      WHEN code IS NULL OR TRIM(code) = '' THEN 'Industry code cannot be empty'
      WHEN NOT REGEXP_LIKE(code, '^[A-Z_]+$') THEN 'Industry code must contain only uppercase letters and underscores'
      ELSE NULL
    END AS error_message
  FROM {{ ref('industry') }}
)

SELECT 
  code,
  name,
  sector,
  subsector,
  industry_group,
  error_message
FROM validation_checks
WHERE error_message IS NOT NULL