select
  c.company_id,
  c.company_name,
  c.country_code,
  c.currency_code
from {{ ref('stg_company') }} c
