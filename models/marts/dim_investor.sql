select
  i.investor_id,
  i.investor_code,
  i.investor_name
from {{ ref('amos_source_example', 'int_entities_investor') }} i
