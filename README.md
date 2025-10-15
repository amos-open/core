# Amos Core dbt Package

**Version: 0.0.1**

This is the core canonical dbt package for the Amos data platform. It transforms staging data from source extensions into canonical business entities and BI-ready marts.

**Note**: This is a dbt package, not a standalone project. Use `amos_runner` to create a new dbt project that includes this package.

## Architecture

The Amos dbt architecture follows a modular, extension-based approach:

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│  Source Extensions  │    │   Core Package      │    │    BI Tools         │
│                     │    │   (this package)    │    │                     │
│ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │
│ │ Raw Sources     │ │───▶│ │ Core Entities   │ │───▶│ │ Dashboards      │ │
│ │ Staging Models  │ │    │ │ Dimensions      │ │    │ │ Reports         │ │
│ └─────────────────┘ │    │ │ Facts           │ │    │ │ Analytics       │ │
│                     │    │ │ Marts           │ │    │ └─────────────────┘ │
│ Examples:           │    │ └─────────────────┘ │    └─────────────────────┘
│ • source_example    │    │                     │
│ • source_snowflake  │    │                     │
│ • source_postgres   │    │                     │
└─────────────────────┘    └─────────────────────┘
```

## Package Structure

This dbt package provides the following model structure:

```
amos_core/             # This package
├── models/
│   ├── core/          # Canonical business layer
│   │   ├── entities/  # Core business entities (fund, company, investor, counterparty)
│   │   ├── dimensions/# Reference/lookup tables (currency, country, industry)
│   │   ├── relationships/ # Entity relationships (commitment, investment, facility)
│   │   ├── bridge/    # Many-to-many relationship tables
│   │   ├── facts/     # Fact tables with metrics and transactions
│   │   └── supporting/# Supporting/detail tables
│   └── marts/         # Final BI-ready models
│       ├── *_metrics.sql # Complex business metrics
│       └── dim_*.sql  # BI dimension tables
├── macros/            # Utility macros
└── dbt_project.yml    # Package configuration
```

When installed in your dbt project via `amos_runner` or `packages.yml`, these models become available for use and extension.

## Installation

### Recommended: Use amos_runner (Starter Project)

The recommended way to get started is to use the `amos_runner` starter project, which creates a new dbt project with `amos_core` and `source_example` as dependencies:

```bash
# Clone the amos_runner starter project
git clone https://github.com/amos/amos_runner.git my_amos_project
cd my_amos_project

# Install dependencies (includes amos_core and source_example)
dbt deps

# Run the complete pipeline
dbt run
dbt test
```

The `amos_runner` starter project includes:
- Pre-configured `packages.yml` with `amos_core` and `source_example`
- Proper `dbt_project.yml` configuration
- Example profiles and connection setup
- Documentation and getting started guide

### Manual Installation (Advanced)

If you prefer to set up manually, create a new dbt project and add dependencies:

```yaml
# packages.yml
packages:
  - package: amos/amos_core
    version: "0.0.1"
  - package: amos/source_example
    version: ">=0.0.1"
```

## Dependencies

This core package requires source extensions to provide:

1. **Raw Sources**: Database table/view definitions
2. **Staging Models**: Clean, standardized data models (e.g., `stg_fund`, `stg_company`)

The `source_example` extension provides sample staging models for development and demo purposes.

## Key Features

### ✅ **Data Contracts**
All core entities use enforced data contracts with:
- Column-level data types and constraints
- Business rule validation
- Foreign key relationships

### ✅ **Performance Optimization**
- Strategic clustering keys for BI queries
- Appropriate materialization strategies
- Incremental loading for large fact tables

### ✅ **Data Quality**
- Comprehensive dbt tests
- Business rule validation
- Referential integrity checks

### ✅ **BI-Ready Outputs**
- Clean, documented marts layer
- Consistent naming conventions
- Business-friendly column descriptions

## Quick Start

### Option 1: amos_runner (Recommended)
```bash
git clone https://github.com/amos/amos_runner.git my_project
cd my_project
dbt deps && dbt run && dbt test
```

### Option 2: Manual Setup
```bash
# Create new dbt project
dbt init my_amos_project
cd my_amos_project

# Add packages.yml with amos_core and source_example
echo "packages:
  - package: amos/amos_core
    version: \"0.0.1\"
  - package: amos/source_example
    version: \">=0.0.1\"" > packages.yml

# Install and run
dbt deps && dbt run && dbt test
```

## Model Naming Conventions

- **Entities**: `fund`, `company`, `investor`, `counterparty`
- **Dimensions**: `dim_currency`, `dim_country`, `dim_industry`
- **Facts**: `fct_transaction`, `fct_fund_snapshot`
- **Relationships**: `commitment`, `investment`, `facility`
- **Marts**: `company_metrics`, `investor_metrics`

## Tags

Models are tagged for easy identification:
- `bi_accessible`: Available for BI consumption
- `canonical`: Core business entities
- `entity`: Master data entities
- `dimension`: Reference/lookup tables
- `fact`: Transactional/event data
- `mart`: Final BI-ready outputs
- `metrics`: Complex calculated metrics

### Version History

- **0.0.1**: Initial release with core entities, dimensions, and marts