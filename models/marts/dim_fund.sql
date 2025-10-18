select
  f.fund_id,
  f.fund_name,
  f.fund_code,
  f.base_currency_code
from {{ ref('amos_source_example', 'int_entities_fund') }} f
