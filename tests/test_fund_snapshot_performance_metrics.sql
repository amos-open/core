-- Test that DPI calculation is consistent with total_distributed and total_called
-- DPI should equal total_distributed / total_called (when total_called > 0)
SELECT 
    fund_id,
    as_of_date,
    dpi,
    total_distributed,
    total_called,
    CASE 
        WHEN total_called > 0 THEN total_distributed / total_called
        ELSE NULL 
    END as calculated_dpi
FROM {{ ref('fund_snapshot') }}
WHERE total_called > 0 
  AND dpi IS NOT NULL
  AND ABS(dpi - (total_distributed / total_called)) > 0.0001  -- Allow for small rounding differences