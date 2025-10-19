{{
  config(
    materialized='incremental',
    unique_key='id',
    cluster_by=['cashflow_date', 'instrument_id'],
    tags=['bi_accessible', 'canonical', 'cashflow'],
    on_schema_change='sync_all_columns'
  )
}}

WITH staging_instrument_cashflow AS (
  SELECT * FROM {{ ref('amos_source_example', 'int_transactions_capital_calls') }}
),

validated_instrument_cashflow AS (
  SELECT
    transaction_id as id,
    null as instrument_id,
    transaction_id,
    transaction_type as cashflow_type,
    transaction_date as cashflow_date,
    CAST(gross_amount_usd AS NUMBER(18,2)) as amount,
    'USD' as currency_code,
    transaction_purpose as description,
    CAST(created_date AS TIMESTAMP_NTZ) as created_at,
    CAST(last_modified_date AS TIMESTAMP_NTZ) as updated_at
  FROM staging_instrument_cashflow
  WHERE 1=1
    -- Basic validation
    AND transaction_id IS NOT NULL
    -- instrument_id may be null for now; relaxed until instrument linking is implemented
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