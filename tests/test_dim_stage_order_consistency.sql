-- Test investment stage order consistency
-- Validates that stage order progression is logical within categories

{{ config(severity = 'error') }}

WITH stage_validation AS (
  SELECT 
    code,
    name,
    stage_category,
    stage_order,
    LAG(stage_order) OVER (PARTITION BY stage_category ORDER BY stage_order) AS prev_stage_order,
    CASE 
      WHEN stage_order IS NULL THEN 'Stage order cannot be null'
      WHEN stage_order <= 0 THEN 'Stage order must be positive'
      WHEN LAG(stage_order) OVER (PARTITION BY stage_category ORDER BY stage_order) IS NOT NULL 
           AND stage_order <= LAG(stage_order) OVER (PARTITION BY stage_category ORDER BY stage_order)
        THEN 'Stage order must be increasing within category'
      ELSE NULL
    END AS error_message
  FROM {{ ref('stage') }}
),

duplicate_orders AS (
  SELECT 
    stage_order,
    COUNT(*) AS order_count
  FROM {{ ref('stage') }}
  GROUP BY stage_order
  HAVING COUNT(*) > 1
)

-- Check for validation errors
SELECT 
  code,
  name,
  stage_category,
  stage_order,
  error_message
FROM stage_validation
WHERE error_message IS NOT NULL

UNION ALL

-- Check for duplicate stage orders
SELECT 
  'DUPLICATE_ORDER' AS code,
  'Multiple stages with same order' AS name,
  'ALL' AS stage_category,
  stage_order,
  'Duplicate stage order found: ' || stage_order AS error_message
FROM duplicate_orders