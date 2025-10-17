-- Test to ensure equity-specific fields are only populated for equity instruments
-- and loan-specific fields are only populated for loan instruments
-- This validates the unified instrument snapshot approach

WITH instrument_snapshot_with_type AS (
  SELECT 
    s.*,
    i.instrument_type
  FROM {{ ref('instrument_snapshot') }} s
  LEFT JOIN {{ ref('instrument') }} i ON s.instrument_id = i.id
),

equity_field_violations AS (
  -- Check that equity fields are not populated for non-equity instruments
  SELECT 
    id,
    instrument_id,
    instrument_type,
    'equity_fields_on_non_equity' as violation_type
  FROM instrument_snapshot_with_type
  WHERE instrument_type NOT IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
    AND (
      equity_stake_pct IS NOT NULL 
      OR equity_dividends_cum IS NOT NULL 
      OR equity_exit_proceeds_actual IS NOT NULL 
      OR equity_exit_proceeds_forecast IS NOT NULL
    )
),

loan_field_violations AS (
  -- Check that loan fields are not populated for non-loan instruments
  -- Note: Basic loan fields are allowed in unified instrument snapshots for all types
  SELECT 
    id,
    instrument_id,
    instrument_type,
    'loan_fields_on_non_loan' as violation_type
  FROM instrument_snapshot_with_type
  WHERE instrument_type != 'LOAN'
    AND (
      principal_outstanding IS NOT NULL 
      OR undrawn_commitment IS NOT NULL 
      OR accrued_income IS NOT NULL 
      OR accrued_fees IS NOT NULL
      OR principal_outstanding_converted IS NOT NULL 
      OR undrawn_commitment_converted IS NOT NULL 
      OR accrued_income_converted IS NOT NULL 
      OR accrued_fees_converted IS NOT NULL
    )
),

equity_company_consistency AS (
  -- Check that equity instruments have company_id populated in the instrument table
  SELECT 
    s.id,
    s.instrument_id,
    i.instrument_type,
    'equity_missing_company' as violation_type
  FROM instrument_snapshot_with_type s
  LEFT JOIN {{ ref('instrument') }} i ON s.instrument_id = i.id
  WHERE i.instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
    AND i.company_id IS NULL
)

SELECT * FROM equity_field_violations
UNION ALL
SELECT * FROM loan_field_violations
UNION ALL
SELECT * FROM equity_company_consistency