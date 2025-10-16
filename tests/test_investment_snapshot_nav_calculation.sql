-- Test that NAV calculation is consistent with unrealized gain/loss and cost basis
-- NAV should equal cost_basis + unrealized_gain_loss
SELECT 
    investment_id,
    as_of_date,
    nav,
    cost_basis,
    unrealized_gain_loss,
    (cost_basis + unrealized_gain_loss) as calculated_nav
FROM {{ ref('investment_snapshot') }}
WHERE nav IS NOT NULL 
  AND cost_basis IS NOT NULL 
  AND unrealized_gain_loss IS NOT NULL
  AND ABS(nav - (cost_basis + unrealized_gain_loss)) > 0.01  -- Allow for small rounding differences