-- Test to ensure all instrument base_currency_code values exist in currency reference table
-- This validates the foreign key relationship

SELECT 
    i.id,
    i.base_currency_code
FROM {{ ref('instrument') }} i
LEFT JOIN {{ ref('currency') }} c ON i.base_currency_code = c.code
WHERE c.code IS NULL