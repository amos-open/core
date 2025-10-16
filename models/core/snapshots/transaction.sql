{{
  config(
    materialized='incremental',
    unique_key='id',
    cluster_by=['transaction_date', 'fund_id', 'tx_type'],
    tags=['bi_accessible', 'canonical', 'transaction'],
    on_schema_change='fail'
  )
}}

WITH staging_transaction AS (
  SELECT * FROM {{ ref('stg_transaction') }}
),

validated_transaction AS (
  SELECT
    id,
    fund_id,
    investment_id,
    loan_id,
    commitment_id,
    tx_type,
    transaction_date,
    amount,
    currency_code,
    description,
    reference_number,
    counterparty_id,
    created_at,
    updated_at
  FROM staging_transaction
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND fund_id IS NOT NULL
    AND tx_type IS NOT NULL
    AND transaction_date IS NOT NULL
    AND amount IS NOT NULL
    AND currency_code IS NOT NULL
    -- Business rule validation
    AND LENGTH(currency_code) = 3
    AND transaction_date <= CURRENT_DATE()
    -- Validate tx_type enum values
    AND tx_type IN (
      'CAPITAL_CALL',
      'DISTRIBUTION',
      'INVESTMENT',
      'DIVESTMENT',
      'LOAN_DRAW',
      'LOAN_REPAYMENT',
      'INTEREST_PAYMENT',
      'FEE_PAYMENT',
      'EXPENSE',
      'INCOME',
      'TRANSFER'
    )
    -- Ensure related entity references are consistent
    AND (
      (tx_type IN ('INVESTMENT', 'DIVESTMENT') AND investment_id IS NOT NULL) OR
      (tx_type IN ('LOAN_DRAW', 'LOAN_REPAYMENT', 'INTEREST_PAYMENT') AND loan_id IS NOT NULL) OR
      (tx_type IN ('CAPITAL_CALL', 'DISTRIBUTION') AND commitment_id IS NOT NULL) OR
      (tx_type IN ('FEE_PAYMENT', 'EXPENSE', 'INCOME', 'TRANSFER'))
    )
)

SELECT
  id,
  fund_id,
  investment_id,
  loan_id,
  commitment_id,
  tx_type,
  transaction_date,
  amount,
  currency_code,
  description,
  reference_number,
  counterparty_id,
  created_at,
  updated_at
FROM validated_transaction

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
     OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}