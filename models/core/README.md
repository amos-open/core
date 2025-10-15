# Core Canonical Layer

This directory contains the canonical dbt models that transform staging data into the core schema.

## Directory Structure

- **dimensions/**: Reference/lookup tables (dim_currency, dim_country, etc.)
- **entities/**: Core business entities (fund, company, investor, counterparty)
- **relationships/**: Entity relationships (commitment, investment, facility, loan, opportunity)
- **bridge/**: Many-to-many relationship tables
- **facts/**: Fact tables with metrics and transactions
- **supporting/**: Supporting/detail tables

## Materialization Strategy

- **Dimensions**: Materialized as tables (small, infrequently changing)
- **Entities**: Materialized as tables (master data requiring fast lookup)
- **Relationships**: Materialized as tables (business relationships)
- **Bridge**: Materialized as tables (many-to-many relationships)
- **Facts**: Materialized incrementally (large, growing datasets)
- **Supporting**: Materialized as tables (detail/supporting data)

## Database Configuration

All core models are configured to be created in:
- **Database**: CORE
- **Schema**: PUBLIC
- **Tags**: ['bi_accessible', 'canonical', '{model_type}']

## Available Macros

- `generate_surrogate_key()`: Creates surrogate keys from multiple columns
- `safe_divide()`: Division with zero/null handling
- `calculate_days_between()`: Date difference calculations
- `format_currency_amount()`: Currency formatting for display

## Data Quality Macros

- `test_not_empty_string()`: Tests for non-empty strings
- `test_valid_uuid()`: Tests for valid UUID format
- `test_positive_amount()`: Tests for positive amounts
- `test_valid_currency_code()`: Tests for valid 3-letter currency codes
- `test_valid_country_code()`: Tests for valid 2-letter country codes

## Enum Validation Approach

Instead of custom enum macros, this project uses standard dbt `accepted_values` tests in schema.yml files:

```yaml
# Example in schema.yml
columns:
  - name: facility_type
    tests:
      - accepted_values:
          values: ['TERM_LOAN_B', 'UNITRANCHE', 'REVOLVER', 'DELAYED_DRAWDOWN', 'MEZZANINE', 'RCF', 'BRIDGE']
```

This approach is:
- Standard dbt practice
- Self-documenting
- Easy to maintain
- Familiar to all dbt developers

## Next Steps

1. Create staging models that these core models will reference
2. Implement the actual SQL model files for each table
3. Add data contracts and comprehensive testing using standard dbt tests
4. Configure incremental loading for fact tables