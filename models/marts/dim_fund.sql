select
  f.fund_id,
  f.fund_name,
  f.fund_code,
  f.base_currency_code
from {{ ref('stg_fund') }} f
