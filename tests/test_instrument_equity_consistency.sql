-- Test to ensure all equity instruments have company_id populated
-- Equity instruments (EQUITY, CONVERTIBLE, WARRANT) must have a company relationship

SELECT 
    id,
    instrument_type,
    company_id
FROM {{ ref('instrument') }}
WHERE instrument_type IN ('EQUITY', 'CONVERTIBLE', 'WARRANT')
  AND company_id IS NULL