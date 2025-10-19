select
  i.id as investor_id,
  i.investor_code,
  i.name as investor_name
from {{ ref('int_entities_investor') }} i
