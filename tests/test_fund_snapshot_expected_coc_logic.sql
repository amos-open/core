-- Test that expected COC is reasonable (DPI + RVPI should approximate expected COC)
-- This is a business logic test to ensure the expected cash-on-cash return makes sense
SELECT 
    fund_id,
    as_of_date,
    dpi,
    rvpi,
    expected_coc,
    (dpi + rvpi) as calculated_total_multiple
FROM {{ ref('fund_snapshot') }}
WHERE dpi IS NOT NULL 
  AND rvpi IS NOT NULL 
  AND expected_coc IS NOT NULL
  -- Expected COC should be reasonably close to DPI + RVPI
  AND ABS(expected_coc - (dpi + rvpi)) > 0.5  -- Allow for reasonable variance in expectations