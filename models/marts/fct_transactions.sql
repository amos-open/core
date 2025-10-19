with t as (
  select * from {{ ref('int_transactions_investments') }}
),
f as (select id as fund_id, admin_fund_code as fund_code from {{ ref('int_entities_fund') }}),
i as (select id as investor_id, investor_code from {{ ref('int_entities_investor') }})
select
  cast(hash(t.source_system, t.transaction_type, t.transaction_id) as varchar) as transaction_id,
  f.fund_id,
  i.investor_id,
  t.investment_date as date,
  t.total_amount_usd as amount,
  'USD' as currency_code,
  t.investment_type as transaction_type,
  t.source_system
from t
left join f on t.canonical_fund_id = f.fund_id
left join i on t.canonical_company_id = i.investor_id
