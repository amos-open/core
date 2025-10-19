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
  SELECT * FROM {{ ref('int_transactions_investments') }}
),

validated_transaction AS (
  SELECT
    transaction_id as id,
    canonical_fund_id as fund_id,
    null as instrument_id,
    null as facility_id,
    null as commitment_id,
    transaction_type,
    transaction_date,
    total_amount_usd as amount,
    'USD' as currency_code,
    null as description,
    null as reference_number,
    null as counterparty_id,
    CAST(created_date AS timestamp_ntz) as created_at,
    CAST(last_modified_date AS timestamp_ntz) as updated_at
  FROM staging_transaction
  WHERE 1=1
    -- Basic validation
    AND transaction_id IS NOT NULL
    AND canonical_fund_id IS NOT NULL
    AND transaction_type IS NOT NULL
    AND transaction_date IS NOT NULL
    AND total_amount_usd IS NOT NULL
    -- Business rule validation (currency_code field not available in staging)
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