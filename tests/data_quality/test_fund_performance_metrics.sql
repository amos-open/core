-- Data Quality Test: Fund Performance Metrics Validation
-- Tests critical business metrics calculations and data consistency

{{ config(
    severity='error',
    tags=['data_quality', 'fund_performance', 'critical']
) }}

-- Test fund performance metric calculations and business rules
WITH fund_performance_issues AS (
    SELECT 
        fs.fund_id,
        f.name as fund_name,
        fs.as_of_date,
        fs.total_nav,
        fs.total_commitment,
        fs.total_called,
        fs.total_distributed,
        fs.dpi,
        fs.rvpi,
        fs.expected_coc,
        -- Calculate expected DPI
        CASE 
            WHEN fs.total_called > 0 THEN fs.total_distributed / fs.total_called 
            ELSE 0 
        END as calculated_dpi,
        -- Calculate expected RVPI
        CASE 
            WHEN fs.total_called > 0 THEN fs.total_nav / fs.total_called 
            ELSE 0 
        END as calculated_rvpi,
        -- Calculate expected COC
        CASE 
            WHEN fs.total_called > 0 THEN (fs.total_nav + fs.total_distributed) / fs.total_called 
            ELSE 0 
        END as calculated_expected_coc,
        -- Identify issues
        CASE 
            WHEN fs.total_called > fs.total_commitment AND fs.total_commitment IS NOT NULL 
                THEN 'Called amount exceeds commitment'
            WHEN fs.total_nav < 0 
                THEN 'Negative NAV'
            WHEN fs.dpi < 0 
                THEN 'Negative DPI'
            WHEN fs.rvpi < 0 
                THEN 'Negative RVPI'
            WHEN fs.expected_coc < 0 
                THEN 'Negative Expected COC'
            WHEN ABS(fs.dpi - (CASE WHEN fs.total_called > 0 THEN fs.total_distributed / fs.total_called ELSE 0 END)) > 0.01 
                THEN 'DPI calculation mismatch'
            WHEN ABS(fs.rvpi - (CASE WHEN fs.total_called > 0 THEN fs.total_nav / fs.total_called ELSE 0 END)) > 0.01 
                THEN 'RVPI calculation mismatch'
            WHEN ABS(fs.expected_coc - (CASE WHEN fs.total_called > 0 THEN (fs.total_nav + fs.total_distributed) / fs.total_called ELSE 0 END)) > 0.01 
                THEN 'Expected COC calculation mismatch'
            WHEN fs.as_of_date > CURRENT_DATE() 
                THEN 'Future snapshot date'
            ELSE NULL
        END as issue_description
    FROM {{ ref('fund_snapshot') }} fs
    JOIN {{ ref('fund') }} f ON fs.fund_id = f.id
    WHERE fs.as_of_date >= CURRENT_DATE() - INTERVAL '90 days'  -- Focus on recent data
)

SELECT 
    fund_id,
    fund_name,
    as_of_date,
    issue_description,
    total_nav,
    total_commitment,
    total_called,
    total_distributed,
    dpi,
    calculated_dpi,
    rvpi,
    calculated_rvpi,
    expected_coc,
    calculated_expected_coc
FROM fund_performance_issues
WHERE issue_description IS NOT NULL