{{
  config(
    materialized='table',
    cluster_by=['facility_id', 'lender_counterparty_id'],
    tags=['bi_accessible', 'canonical', 'bridge']
  )
}}

WITH staging_facility_lender AS (
  SELECT * FROM {{ ref('stg_facility_lender') }}
),

validated_facility_lender AS (
  SELECT
    facility_id,
    lender_counterparty_id,
    syndicate_role,
    allocation_pct,
    commitment_amount,
    created_at,
    updated_at
  FROM staging_facility_lender
  WHERE 1=1
    -- Basic validation
    AND facility_id IS NOT NULL
    AND lender_counterparty_id IS NOT NULL
    AND syndicate_role IS NOT NULL
    -- Business rule validation
    AND allocation_pct >= 0
    AND allocation_pct <= 100
    AND commitment_amount >= 0
    -- Enum validation for syndicate_role
    AND syndicate_role IN ('AGENT', 'LEAD_ARRANGER', 'BOOKRUNNER', 'PARTICIPANT', 'CLUB_MEMBER')
)

SELECT
  facility_id,
  lender_counterparty_id,
  syndicate_role,
  allocation_pct,
  commitment_amount,
  created_at,
  updated_at
FROM validated_facility_lender