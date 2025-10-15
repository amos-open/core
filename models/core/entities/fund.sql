-- Fund entity model with performance attributes
-- Transforms staging fund data to canonical schema with data contracts

{{ config(
    materialized='table',
    cluster_by=['base_currency_code', 'fund_type'],
    tags=['bi_accessible', 'canonical', 'entity']
) }}

WITH fund_base AS (
    SELECT 
        id,
        name,
        base_currency_code,
        fund_type,
        status,
        target_size,
        committed_capital,
        called_capital,
        inception_date,
        maturity_date,
        description,
        created_at,
        updated_at
    FROM {{ ref('stg_fund') }}
),

fund_with_metrics AS (
    SELECT 
        id,
        name,
        base_currency_code,
        fund_type,
        status,
        target_size,
        committed_capital,
        called_capital,
        inception_date,
        maturity_date,
        description,
        created_at,
        updated_at,
        
        -- Performance calculations
        CASE 
            WHEN target_size > 0 THEN (committed_capital / target_size) * 100
            ELSE 0 
        END as commitment_percentage,
        
        CASE 
            WHEN committed_capital > 0 THEN (called_capital / committed_capital) * 100
            ELSE 0 
        END as drawdown_percentage,
        
        -- Fund age in years
        DATEDIFF('year', inception_date, CURRENT_DATE()) as fund_age_years,
        
        -- Remaining term in years
        DATEDIFF('year', CURRENT_DATE(), maturity_date) as remaining_term_years
        
    FROM fund_base
)

SELECT 
    id,
    name,
    base_currency_code,
    fund_type,
    status,
    target_size,
    committed_capital,
    called_capital,
    inception_date,
    maturity_date,
    description,
    commitment_percentage,
    drawdown_percentage,
    fund_age_years,
    remaining_term_years,
    created_at,
    updated_at
FROM fund_with_metrics

-- Data quality validation
WHERE id IS NOT NULL
  AND name IS NOT NULL
  AND base_currency_code IS NOT NULL
  AND target_size >= 0
  AND committed_capital >= 0
  AND called_capital >= 0
  AND called_capital <= committed_capital
  AND committed_capital <= target_size