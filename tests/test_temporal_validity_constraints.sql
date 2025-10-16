-- Test that temporal validity constraints are properly enforced across all temporal bridge tables
-- This test ensures that valid_to is always greater than valid_from when not null

WITH facility_country_invalid AS (
  SELECT 
    'facility_country' as table_name,
    facility_id as entity_id,
    country_code as related_id,
    valid_from,
    valid_to
  FROM {{ ref('facility_country') }}
  WHERE valid_to IS NOT NULL AND valid_to <= valid_from
),

facility_industry_invalid AS (
  SELECT 
    'facility_industry' as table_name,
    facility_id as entity_id,
    industry_id as related_id,
    valid_from,
    valid_to
  FROM {{ ref('facility_industry') }}
  WHERE valid_to IS NOT NULL AND valid_to <= valid_from
),

loan_country_invalid AS (
  SELECT 
    'loan_country' as table_name,
    loan_id as entity_id,
    country_code as related_id,
    valid_from,
    valid_to
  FROM {{ ref('loan_country') }}
  WHERE valid_to IS NOT NULL AND valid_to <= valid_from
),

loan_industry_invalid AS (
  SELECT 
    'loan_industry' as table_name,
    loan_id as entity_id,
    industry_id as related_id,
    valid_from,
    valid_to
  FROM {{ ref('loan_industry') }}
  WHERE valid_to IS NOT NULL AND valid_to <= valid_from
),

all_invalid AS (
  SELECT * FROM facility_country_invalid
  UNION ALL
  SELECT * FROM facility_industry_invalid
  UNION ALL
  SELECT * FROM loan_country_invalid
  UNION ALL
  SELECT * FROM loan_industry_invalid
)

SELECT * FROM all_invalid