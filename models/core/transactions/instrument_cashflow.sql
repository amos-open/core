{{
  config(
    materialized='incremental',
    unique_key='id',
    cluster_by=['cashflow_date', 'instrument_id'],
    tags=['bi_accessible', 'canonical', 'cashflow'],
    on_schema_change='fail'
  )
}}

WITH staging_instrument_cashflow AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_transactions_capital_calls') }}
),

validated_instrument_cashflow AS (
  SELECT
    id,
    instrument_id,
    transaction_id,
    cashflow_type,
    cashflow_date,
    amount,
    currency_code,
    description,
    created_at,
    updated_at
  FROM staging_instrument_cashflow
  WHERE 1=1
    -- Basic validation
    AND id IS NOT NULL
    AND instrument_id IS NOT NULL
    AND cashflow_type IS NOT NULL
    AND cashflow_date IS NOT NULL
    AND amount IS NOT NULL
    AND currency_code IS NOT NULL
    -- Business rule validation
    AND LENGTH(currency_code) = 3
    AND cashflow_date <= CURRENT_DATE()
    -- Validate cashflow_type enum values per DBML specification
    AND cashflow_type IN (
      'CONTRIBUTION',
      'DISTRIBUTION', 
      'DIVIDEND',
      'INTEREST',
      'FEE',
      'PRINCIPAL',
      'DRAW',
      'PREPAYMENT',
      'OTHER'
    )
)

SELECT
  id,
  instrument_id,
  transaction_id,
  cashflow_type,
  cashflow_date,
  amount,
  currency_code,
  description,
  created_at,
  updated_at
FROM validated_instrument_cashflow

{% if is_incremental() %}
  -- Only process new or updated records
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
     OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}