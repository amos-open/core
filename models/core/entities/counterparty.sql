-- Counterparty entity model for external parties
-- Transforms staging counterparty data to canonical schema with business constraints

{{ config(
    materialized='table',
    cluster_by=['counterparty_type', 'country_code'],
    tags=['bi_accessible', 'canonical', 'entity']
) }}

WITH counterparty_base AS (
    SELECT 
        id,
        name,
        counterparty_type,
        status,
        country_code,
        description,
        created_at,
        updated_at
    FROM {{ ref('stg_counterparty') }}
),

SELECT 
    id,
    name,
    counterparty_type,
    status,
    country_code,
    description,
    created_at,
    updated_at
FROM counterparty_base

-- Data quality validation
WHERE id IS NOT NULL
  AND name IS NOT NULL
  AND counterparty_type IS NOT NULL
  AND status IS NOT NULL