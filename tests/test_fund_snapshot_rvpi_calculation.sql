-- Test that RVPI calculation is consistent with total_nav and total_called
-- RVPI should equal total_nav / total_called (when total_called > 0)
SELECT 
    fund_id,
    as_of_date,
    rvpi,
    total_nav,
    total_called,
    CASE 
        WHEN total_called > 0 THEN total_nav / total_called
        ELSE NULL 
    END as calculated_rvpi
FROM {{ ref('fund_snapshot') }}
WHERE total_called > 0 
  AND rvpi IS NOT NULL
  AND ABS(rvpi - (total_nav / total_called)) > 0.0001  -- Allow for small rounding differences