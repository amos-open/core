select
  f.id as fund_id,
  f.name as fund_name,
  f.admin_fund_code as fund_code,
  f.base_currency_code
from {{ ref('int_entities_fund') }} f
