-- Test industry classification completeness
-- Ensures all required industry classifications are present

{{ config(severity = 'warn') }}

WITH required_sectors AS (
  SELECT 'Technology' AS sector
  UNION ALL SELECT 'Healthcare'
  UNION ALL SELECT 'Financials'
  UNION ALL SELECT 'Energy'
  UNION ALL SELECT 'Consumer Discretionary'
  UNION ALL SELECT 'Industrials'
  UNION ALL SELECT 'Real Estate'
),

current_sectors AS (
  SELECT DISTINCT sector
  FROM {{ ref('industry') }}
  WHERE is_active = TRUE
),

missing_sectors AS (
  SELECT r.sector
  FROM required_sectors r
  LEFT JOIN current_sectors c ON r.sector = c.sector
  WHERE c.sector IS NULL
)

SELECT 
  sector,
  'Missing required sector in industry dimension' AS error_message
FROM missing_sectors