select
  c.company_id,
  c.company_name,
  c.country_code,
  c.currency_code
from {{ ref('amos_source_example', 'int_entities_company') }} c
