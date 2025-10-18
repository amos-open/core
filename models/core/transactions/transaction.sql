{{
  config(
    materialized='incremental',
    unique_key='id',
    cluster_by=['transaction_date', 'fund_id', 'transaction_type'],
    tags=['bi_accessible', 'canonical', 'transaction'],
    on_schema_change='fail'
  )
}}

WITH staging_transaction AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_transactions_investments') }}
),

validated_transaction AS (
  SELECT
    id,
    fund_id,
    instrument_id,
    facility_id,
    commitment_id,
    transaction_type,
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
    AND transaction_type IS NOT NULL
    AND transaction_date IS NOT NULL
    AND amount IS NOT NULL
    AND currency_code IS NOT NULL
    -- Business rule validation
    AND LENGTH(currency_code) = 3
    AND transaction_date <= CURRENT_DATE()
    -- Validate transaction_type enum values per DBML specification
    AND transaction_type IN (
      'DRAWDOWN',
      'DISTRIBUTION',
      'DIVIDEND',
      'INVESTMENT_TRANSACTION',
      'EXPENSE',
      'MANAGEMENT_FEE',
      'LOAN_RECEIVED',
      'LOAN_DRAW',
      'LOAN_PRINCIPAL_REPAYMENT',
      'LOAN_INTEREST_RECEIPT',
      'LOAN_FEE_RECEIPT'
    )
    -- Ensure related entity references are consistent
    AND (
      (transaction_type IN ('INVESTMENT_TRANSACTION', 'DIVIDEND') AND instrument_id IS NOT NULL) OR
      (transaction_type IN ('LOAN_RECEIVED', 'LOAN_DRAW', 'LOAN_PRINCIPAL_REPAYMENT', 'LOAN_INTEREST_RECEIPT', 'LOAN_FEE_RECEIPT') AND (instrument_id IS NOT NULL OR facility_id IS NOT NULL)) OR
      (transaction_type IN ('DRAWDOWN', 'DISTRIBUTION') AND commitment_id IS NOT NULL) OR
      (transaction_type IN ('EXPENSE', 'MANAGEMENT_FEE'))
    )
)

SELECT
  id,
  fund_id,
  instrument_id,
  facility_id,
  commitment_id,
  transaction_type,
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