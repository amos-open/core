-- Test geographic hierarchy consistency for country dimension
-- Validates that region and subregion combinations are valid

{{ config(severity = 'warn') }}

WITH known_region_subregion_combinations AS (
  SELECT 'Europe' AS region, 'Northern Europe' AS subregion
  UNION ALL SELECT 'Europe', 'Western Europe'
  UNION ALL SELECT 'Europe', 'Eastern Europe'
  UNION ALL SELECT 'Europe', 'Southern Europe'
  UNION ALL SELECT 'North America', 'Northern America'
  UNION ALL SELECT 'North America', 'Central America'
  UNION ALL SELECT 'South America', 'South America'
  UNION ALL SELECT 'Asia', 'Eastern Asia'
  UNION ALL SELECT 'Asia', 'Western Asia'
  UNION ALL SELECT 'Asia', 'Southern Asia'
  UNION ALL SELECT 'Asia', 'South-Eastern Asia'
  UNION ALL SELECT 'Asia', 'Central Asia'
  UNION ALL SELECT 'Africa', 'Northern Africa'
  UNION ALL SELECT 'Africa', 'Western Africa'
  UNION ALL SELECT 'Africa', 'Eastern Africa'
  UNION ALL SELECT 'Africa', 'Southern Africa'
  UNION ALL SELECT 'Africa', 'Middle Africa'
  UNION ALL SELECT 'Oceania', 'Australia and New Zealand'
  UNION ALL SELECT 'Oceania', 'Polynesia'
  UNION ALL SELECT 'Oceania', 'Melanesia'
  UNION ALL SELECT 'Oceania', 'Micronesia'
),

country_validation AS (
  SELECT 
    c.code,
    c.name,
    c.region,
    c.subregion,
    CASE 
      WHEN k.region IS NULL THEN 'Invalid region/subregion combination: ' || c.region || '/' || c.subregion
      ELSE NULL
    END AS error_message
  FROM {{ ref('country') }} c
  LEFT JOIN known_region_subregion_combinations k 
    ON c.region = k.region AND c.subregion = k.subregion
)

SELECT 
  code,
  name,
  region,
  subregion,
  error_message
FROM country_validation
WHERE error_message IS NOT NULL