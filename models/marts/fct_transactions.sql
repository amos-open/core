with t as (
  select * from {{ ref('stg_transactions') }}
),
f as (select fund_id, fund_code from {{ ref('stg_fund') }}),
i as (select investor_id, investor_code from {{ ref('stg_investor') }})
select
  {{ dbt_utils.generate_surrogate_key(['t.source_system','t.transaction_type','t.natural_key']) }} as transaction_id,
  f.fund_id,
  i.investor_id,
  t.date,
  t.amount,
  t.currency_code,
  t.transaction_type,
  t.source_system
from t
left join f on t.fund_code = f.fund_code
left join i on t.investor_code = i.investor_code
