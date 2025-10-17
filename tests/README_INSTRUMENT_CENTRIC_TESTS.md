# Instrument-Centric Test Suite

This document summarizes the test updates made to support the new unified instrument-centric approach in the amos_core dbt package.

## Overview

The test suite has been updated to validate the new unified instrument model that abstracts both equity and loan instruments into a single `instrument` table, with product-specific attributes maintained in related tables.

## Updated Tests

### 1. Core Instrument Validation Tests

#### `test_instrument_type_distribution.sql`
- **Purpose**: Validates instrument_type enum values match DBML specification
- **Updates**: Enhanced to check for invalid instrument types and provide distribution statistics
- **Validates**: EQUITY, LOAN, CONVERTIBLE, WARRANT, FUND_INTEREST enum values

#### `test_instrument_business_rules.sql` (NEW)
- **Purpose**: Comprehensive business rule validation for unified instrument model
- **Validates**:
  - Equity instruments must have company_id
  - Fund interest instruments should not have company_id
  - Valid date ranges (termination > inception)
  - Valid fund and company references

#### `test_instrument_equity_consistency.sql`
- **Purpose**: Ensures equity instruments have proper company relationships
- **Status**: Already aligned with instrument-centric approach
- **Validates**: Equity instruments (EQUITY, CONVERTIBLE, WARRANT) have company_id populated

### 2. Instrument Snapshot Tests

#### `test_instrument_snapshot_field_validation.sql`
- **Purpose**: Validates field population based on instrument type
- **Updates**: Enhanced to include equity-company consistency checks
- **Validates**:
  - Equity fields only populated for equity instruments
  - Loan fields only populated for loan instruments
  - Equity instruments have valid company references

#### `test_instrument_snapshot_currency_conversion_validation.sql`
- **Purpose**: Validates currency conversion logic in unified snapshots
- **Updates**: Renamed from investment_snapshot test, enhanced validation
- **Validates**: Converted amounts = original amounts × fx_rate

### 3. Transaction Tests

#### `test_transaction_categorization_logic.sql`
- **Purpose**: Validates transaction entity references
- **Updates**: Updated to use instrument_id instead of investment_id/loan_id
- **Validates**:
  - Instrument transactions have instrument_id
  - Loan transactions have instrument_id or facility_id
  - Capital transactions have commitment_id

#### `test_transaction_amount_sign_logic.sql`
- **Purpose**: Validates transaction amount signs based on type
- **Updates**: Updated transaction types to match new DBML specification
- **Validates**: Appropriate signs for inflow/outflow transactions

### 4. Bridge Table Tests

#### `test_instrument_country_allocation_totals.sql`
- **Purpose**: Validates country allocation percentages sum to 100%
- **Status**: Already aligned with instrument-centric approach
- **Validates**: Temporal allocation consistency

#### `test_instrument_industry_allocation_totals.sql`
- **Purpose**: Validates industry allocation percentages sum to 100%
- **Status**: Already aligned with instrument-centric approach
- **Validates**: Temporal allocation consistency

#### `test_instrument_country_primary_flag_consistency.sql`
- **Purpose**: Validates exactly one primary country per instrument per period
- **Status**: Already aligned with instrument-centric approach

#### `test_instrument_industry_primary_flag_consistency.sql`
- **Purpose**: Validates exactly one primary industry per instrument per period
- **Status**: Already aligned with instrument-centric approach

### 5. Cashflow Tests

#### `test_instrument_cashflow_consistency.sql` (NEW)
- **Purpose**: Validates unified instrument cashflow approach
- **Validates**:
  - Valid instrument and transaction references
  - Cashflow type enum values
  - Amount consistency with linked transactions
  - Appropriate cashflow types for instrument types

#### `test_instrument_cashflow_type_validation.sql` (NEW)
- **Purpose**: Validates cashflow_type enum values
- **Validates**: CONTRIBUTION, DISTRIBUTION, DIVIDEND, INTEREST, FEE, PRINCIPAL, DRAW, PREPAYMENT, OTHER

### 6. Temporal Validation Tests

#### `test_temporal_validity_constraints.sql`
- **Purpose**: Validates temporal bridge table constraints
- **Updates**: Updated to work with instrument_country and instrument_industry tables
- **Validates**:
  - valid_to > valid_from when not null
  - No overlapping periods for same instrument-entity combinations

## Removed/Deprecated Tests

The following test patterns were removed as they referenced deleted loan/facility models:
- Direct references to loan_id in transaction tests (replaced with instrument_id)
- Facility-specific allocation tests (consolidated into instrument tests)
- Investment-specific tests (consolidated into instrument tests)

## Test Coverage

The updated test suite provides comprehensive coverage for:

1. **DBML Compliance**: All enum values and constraints match DBML specification
2. **Business Rules**: Instrument-specific business logic validation
3. **Data Quality**: Referential integrity and consistency checks
4. **Temporal Logic**: Valid date ranges and allocation consistency
5. **Currency Handling**: Proper currency conversion validation
6. **Unified Approach**: Tests work across both equity and loan instruments

## Running the Tests

All tests are designed to work with the standard dbt test framework:

```bash
# Run all tests
dbt test

# Run instrument-specific tests
dbt test --select tag:instrument

# Run specific test categories
dbt test --select test_instrument_*
```

## Notes

- Tests are designed to return empty result sets when passing
- All enum validations use dbt `accepted_values` tests in schema.yml files
- Temporal tests account for null valid_to dates (representing current/ongoing periods)
- Currency conversion tests allow for small rounding differences (0.01 threshold)