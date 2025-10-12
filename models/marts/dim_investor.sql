select
  i.investor_id,
  i.investor_code,
  i.investor_name
from {{ ref('stg_investor') }} i
