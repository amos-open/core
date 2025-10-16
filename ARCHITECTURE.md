# Amos dbt Architecture

## Overview

This document explains the Amos dbt architecture and the proper separation of concerns between source extensions and the core canonical package.

## Architecture Principles

### 1. **Extension-Based Sources**
- Raw sources and staging models belong to **source extensions**
- Extensions are installed as dbt packages (e.g., `source_example`, `source_snowflake`)
- Each extension provides clean, standardized staging models

### 2. **Core Canonical Layer**
- Core package contains **business entities**, **dimensions**, and **relationships**
- References staging models from extensions via `{{ ref('stg_model') }}`
- Focuses on business logic, not data extraction/cleaning

### 3. **Marts for BI**
- Final layer contains **BI-ready models** for consumption
- Complex metrics and aggregations live here
- Direct interface for dashboards and analytics

## Directory Structure

```
# Source Extensions (separate dbt packages)
source_example/
├── models/
│   └── staging/
│       ├── stg_fund.sql
│       ├── stg_company.sql
│       ├── stg_investor.sql
│       └── stg_counterparty.sql
└── sources/
    └── sources.yml

# Core Package (this repository)
amos_core/
├── models/
│   ├── core/
│   │   ├── entities/        # fund, company, investor, counterparty
│   │   ├── dimensions/      # dim_currency, dim_country, dim_industry
│   │   ├── relationships/   # commitment, investment, opportunity
│   │   ├── bridge/          # many-to-many relationships
│   │   ├── facts/           # fct_transaction, fct_fund_snapshot
│   │   └── supporting/      # supporting detail tables
│   └── marts/
│       ├── company_metrics.sql
│       ├── investor_metrics.sql
│       └── counterparty_metrics.sql
└── README.md
```

## Data Flow

```
Raw Sources (Extension) → Staging (Extension) → Core (This Package) → Marts (This Package) → BI Tools
```

1. **Extensions** extract and clean raw data into staging models
2. **Core entities** transform staging data into canonical business objects
3. **Marts** aggregate and calculate complex metrics for BI consumption

## Benefits

### ✅ **Modularity**
- Source systems can be swapped by changing extensions
- Core business logic remains stable
- Easy to add new data sources

### ✅ **Separation of Concerns**
- Extensions handle data extraction/cleaning
- Core handles business logic/relationships
- Marts handle BI-specific aggregations

### ✅ **Reusability**
- Core package works with any compatible source extension
- Extensions can be shared across dbt projects
- Business logic is centralized

### ✅ **Maintainability**
- Source changes only affect extensions
- Business rule changes only affect core
- BI changes only affect marts

## Implementation Notes

### Core Entity Models
- Reference staging models: `FROM {{ ref('stg_fund') }}`
- Contain simple derived attributes (age calculations, percentages)
- Enforce data contracts and business rules
- Use appropriate materialization strategies

### Marts Models
- Contain complex business metrics and aggregations
- Final BI-ready outputs with business-friendly naming
- Tagged as `mart` and `metrics` for identification
- Include audit fields (`calculated_at`)

### Extension Requirements
Source extensions must provide:
- Clean, standardized column names
- Basic data quality (null handling, type casting)
- Consistent grain and business keys
- Proper documentation

## Example Usage

### Recommended: Use amos_runner Starter Project
```bash
# Clone starter project with pre-configured dependencies
git clone https://github.com/amos/amos_runner.git my_amos_project
cd my_amos_project

# Install dependencies (amos_core + source_example)
dbt deps

# Run complete pipeline
dbt run && dbt test
```

### Manual Setup
```yaml
# packages.yml
packages:
  - package: amos/amos_core
    version: "0.0.1"
  - package: amos/source_example
    version: ">=1.0.0"
```

### Running Your dbt Project
```bash
# Install dependencies (including extensions)
dbt deps

# Run staging models (from extensions) + core + marts
dbt run

# Test data quality across all layers
dbt test
```

### Querying Results
```sql
-- Core entity (clean, simple)
SELECT * FROM {{ ref('fund') }}

-- BI-ready metrics (complex, aggregated)
SELECT * FROM {{ ref('company_metrics') }}
```

This architecture ensures clean separation of concerns while maintaining flexibility and reusability across the Amos data platform.