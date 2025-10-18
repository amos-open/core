-- Test KYC requirements consistency for investor types
-- Validates that KYC requirements align with investor categories

{{ config(severity = 'warn') }}

WITH kyc_validation AS (
  SELECT 
    code,
    name,
    category,
    requires_kyc,
    CASE 
      -- Professional and institutional investors should typically require KYC
      WHEN category IN ('PROFESSIONAL', 'GOVERNMENT', 'FINANCIAL_INSTITUTION') AND requires_kyc = FALSE 
        THEN 'Professional/institutional investors should typically require KYC'
      -- Individual retail investors may not require enhanced KYC
      WHEN category = 'INDIVIDUAL' AND code = 'RETAIL' AND requires_kyc = TRUE
        THEN 'Retail investors may not require enhanced KYC procedures'
      ELSE NULL
    END AS warning_message
  FROM {{ ref('investor_type') }}
)

SELECT 
  code,
  name,
  category,
  requires_kyc,
  warning_message
FROM kyc_validation
WHERE warning_message IS NOT NULL