-- Test that each facility has exactly one AGENT
-- This test ensures business rule compliance for syndicate structure

WITH facility_agent_counts AS (
  SELECT 
    facility_id,
    COUNT(*) as agent_count
  FROM {{ ref('facility_lender') }}
  WHERE syndicate_role = 'AGENT'
  GROUP BY facility_id
),

invalid_agent_counts AS (
  SELECT 
    facility_id,
    agent_count
  FROM facility_agent_counts
  WHERE agent_count != 1
)

SELECT * FROM invalid_agent_counts