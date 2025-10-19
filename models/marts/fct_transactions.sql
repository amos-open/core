with t as (
  select * from {{ ref('int_transactions_investments') }}
),
f as (select id as fund_id, admin_fund_code as fund_code from {{ ref('int_entities_fund') }}),
i as (select id as investor_id, investor_code from {{ ref('int_entities_investor') }})
select
  cast(hash(t.source_system, t.transaction_type, t.transaction_id) as varchar) as transaction_id,
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
