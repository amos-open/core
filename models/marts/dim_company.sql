select
  c.id as company_id,
  c.name as company_name,
  c.country_code,
  null as currency_code
from {{ ref('int_entities_company') }} c
