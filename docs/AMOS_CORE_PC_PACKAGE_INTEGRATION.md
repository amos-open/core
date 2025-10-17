# amos_core and PC Package Integration Guide

## Overview

This document explains the relationship between the amos_core canonical data model and the future Private Credit (PC) package, detailing how the instrument-centric design enables seamless integration while maintaining clear separation of concerns.

## Architecture Overview

### Current State: amos_core Foundation

The amos_core package provides the unified foundation for all financial instruments:

```
amos_core (Current Implementation)
├── Unified Instrument Model
│   ├── instrument (all types: EQUITY, LOAN, CONVERTIBLE, etc.)
│   ├── instrument_snapshot (unified valuations)
│   ├── instrument_cashflow (unified cashflows)
│   └── instrument_country/industry (unified allocations)
├── Core Entities
│   ├── fund, company, investor, counterparty
│   └── currency, country, industry (reference data)
├── Basic Loan Support
│   ├── Basic loan fields in instrument_snapshot
│   ├── Loan transactions and cashflows
│   └── Geographic/industry allocations for loans
└── Equity Full Support
    ├── Complete equity analysis capabilities
    ├── Company relationships and performance
    └── Equity-specific metrics and reporting
```

### Future State: PC Package Extension

The PC package will extend amos_core with detailed private credit capabilities:

```
PC Package (Future Implementation)
├── Detailed Loan Models
│   ├── loan (detailed loan attributes)
│   ├── facility (credit facility management)
│   ├── loan_covenant (covenant tracking)
│   └── loan_pricing (pricing and terms)
├── Advanced Credit Analytics
│   ├── credit_rating (internal ratings)
│   ├── loan_performance (detailed metrics)
│   ├── covenant_compliance (monitoring)
│   └── credit_risk_metrics (risk calculations)
├── Specialized Snapshots
│   ├── loan_snapshot (detailed loan metrics)
│   ├── facility_snapshot (facility utilization)
│   └── covenant_snapshot (compliance tracking)
└── Integration Layer
    ├── References amos_core instruments
    ├── Extends basic loan functionality
    └── Maintains unified reporting interface
```

## Integration Design Principles

### 1. Unified Interface Principle

**amos_core provides the unified interface for all reporting and analysis:**

- All instruments (equity and loan) accessible through `instrument` table
- Unified performance metrics through `instrument_snapshot`
- Consistent geographic/industry analysis across all instrument types
- Single transaction and cashflow interface

**PC package extends without breaking the interface:**

- Detailed loan data supplements (not replaces) amos_core data
- Advanced analytics build on top of unified foundation
- Specialized reporting uses both packages together

### 2. Separation of Concerns

**amos_core responsibilities:**
- Core instrument abstraction and unified interface
- Basic loan support for portfolio-level analysis
- Cross-product reporting and analytics
- Regulatory and compliance reporting foundation

**PC package responsibilities:**
- Detailed loan and facility management
- Advanced credit risk analytics
- Covenant monitoring and compliance
- Specialized private credit reporting

### 3. Data Flow Architecture

```
Source Systems
    ↓
Staging Layer
    ↓
amos_core (Canonical Layer)
    ├── Basic loan data for unified reporting
    └── Complete equity data
    ↓
PC Package (Specialized Layer)
    ├── Detailed loan extensions
    └── Advanced credit analytics
    ↓
Marts Layer
    ├── Unified portfolio reports (amos_core)
    ├── Equity-specific reports (amos_core)
    └── Credit-specific reports (PC package + amos_core)
```

## Current amos_core Loan Capabilities

### Basic Loan Support in amos_core

The current implementation provides essential loan functionality for unified portfolio analysis:

#### 1. Instrument Table Support
```sql
-- Loan instruments in unified instrument table
SELECT 
    id,
    fund_id,
    instrument_type,  -- 'LOAN'
    base_currency_code,
    inception_date,
    termination_date,
    description
FROM {{ ref('instrument') }}
WHERE instrument_type = 'LOAN';
```

#### 2. Basic Loan Snapshots
```sql
-- Basic loan metrics in instrument_snapshot
SELECT 
    instrument_id,
    as_of_date,
    fair_value_converted,
    amortized_cost_converted,
    principal_outstanding_converted,
    undrawn_commitment_converted,
    accrued_income_converted,
    accrued_fees_converted
FROM {{ ref('instrument_snapshot') }} s
JOIN {{ ref('instrument') }} i ON s.instrument_id = i.id
WHERE i.instrument_type = 'LOAN';
```

#### 3. Loan Transactions and Cashflows
```sql
-- Loan-related transactions
SELECT 
    t.transaction_type,  -- LOAN_RECEIVED, LOAN_DRAW, etc.
    t.amount,
    t.transaction_date
FROM {{ ref('transaction') }} t
JOIN {{ ref('instrument') }} i ON t.instrument_id = i.id
WHERE i.instrument_type = 'LOAN';

-- Loan cashflows
SELECT 
    ic.cashflow_type,  -- DRAW, PRINCIPAL, INTEREST, FEE
    ic.amount,
    ic.cashflow_date
FROM {{ ref('instrument_cashflow') }} ic
JOIN {{ ref('instrument') }} i ON ic.instrument_id = i.id
WHERE i.instrument_type = 'LOAN';
```

#### 4. Geographic and Industry Allocations
```sql
-- Loan geographic exposure (same interface as equity)
SELECT 
    i.id as loan_id,
    co.name as country_name,
    ic.allocation_pct,
    ic.role  -- DOMICILE, OPERATIONS, etc.
FROM {{ ref('instrument') }} i
JOIN {{ ref('instrument_country') }} ic ON i.id = ic.instrument_id
JOIN {{ ref('country') }} co ON ic.country_code = co.code
WHERE i.instrument_type = 'LOAN'
  AND ic.valid_to IS NULL;
```

### Limitations of Current Loan Support

The amos_core implementation provides basic loan support but has intentional limitations:

1. **No Detailed Loan Attributes**: Complex loan terms, covenants, pricing details
2. **No Facility Management**: Credit facility structures, utilization tracking
3. **No Advanced Credit Analytics**: Credit ratings, risk metrics, covenant compliance
4. **No Specialized Loan Reporting**: Detailed credit portfolio analysis

These limitations are by design - they will be addressed by the PC package.

## Future PC Package Integration

### Integration Patterns

#### 1. Reference-Based Integration
```sql
-- PC package loan table references amos_core instrument
CREATE TABLE pc.loan (
    id VARCHAR(36) PRIMARY KEY,
    instrument_id VARCHAR(36) NOT NULL,  -- References amos_core.instrument
    loan_type VARCHAR(50),
    credit_rating VARCHAR(10),
    covenant_count INTEGER,
    -- ... detailed loan attributes
    FOREIGN KEY (instrument_id) REFERENCES amos_core.instrument(id)
);
```

#### 2. Extended Snapshot Integration
```sql
-- PC package provides detailed loan snapshots
CREATE TABLE pc.loan_snapshot (
    id VARCHAR(36) PRIMARY KEY,
    loan_id VARCHAR(36) NOT NULL,
    instrument_id VARCHAR(36) NOT NULL,  -- Link to amos_core
    as_of_date DATE NOT NULL,
    -- Detailed loan metrics
    credit_spread DECIMAL(10,4),
    probability_of_default DECIMAL(5,4),
    loss_given_default DECIMAL(5,4),
    -- ... advanced metrics
    FOREIGN KEY (instrument_id) REFERENCES amos_core.instrument(id)
);
```

#### 3. Unified Reporting Integration
```sql
-- Combined reporting using both packages
SELECT 
    -- Core instrument data (amos_core)
    i.id,
    i.description,
    f.name as fund_name,
    s.fair_value_converted,
    s.principal_outstanding_converted,
    
    -- Detailed loan data (PC package)
    l.loan_type,
    l.credit_rating,
    ls.credit_spread,
    ls.probability_of_default
    
FROM amos_core.instrument i
JOIN amos_core.fund f ON i.fund_id = f.id
JOIN amos_core.instrument_snapshot s ON i.id = s.instrument_id
LEFT JOIN pc.loan l ON i.id = l.instrument_id
LEFT JOIN pc.loan_snapshot ls ON l.id = ls.loan_id 
    AND ls.as_of_date = s.as_of_date
WHERE i.instrument_type = 'LOAN';
```

### Data Consistency Patterns

#### 1. Single Source of Truth
- **amos_core**: Authoritative for basic instrument data, valuations, cashflows
- **PC package**: Authoritative for detailed loan attributes, advanced analytics
- **No Duplication**: PC package extends (not duplicates) amos_core data

#### 2. Referential Integrity
```sql
-- PC package maintains referential integrity with amos_core
ALTER TABLE pc.loan 
ADD CONSTRAINT fk_loan_instrument 
FOREIGN KEY (instrument_id) 
REFERENCES amos_core.instrument(id);

-- Ensure loan records only reference LOAN instruments
ALTER TABLE pc.loan 
ADD CONSTRAINT chk_loan_instrument_type
CHECK (
    instrument_id IN (
        SELECT id FROM amos_core.instrument 
        WHERE instrument_type = 'LOAN'
    )
);
```

#### 3. Temporal Consistency
```sql
-- Ensure snapshot dates align between packages
CREATE VIEW pc.unified_loan_snapshot AS
SELECT 
    s.instrument_id,
    s.as_of_date,
    -- amos_core metrics
    s.fair_value_converted,
    s.principal_outstanding_converted,
    -- PC package metrics
    ls.credit_spread,
    ls.probability_of_default
FROM amos_core.instrument_snapshot s
JOIN amos_core.instrument i ON s.instrument_id = i.id
LEFT JOIN pc.loan l ON i.id = l.instrument_id
LEFT JOIN pc.loan_snapshot ls ON l.id = ls.loan_id 
    AND ls.as_of_date = s.as_of_date
WHERE i.instrument_type = 'LOAN';
```

## Migration and Implementation Strategy

### Phase 1: amos_core Foundation (Current)
- ✅ Unified instrument model implemented
- ✅ Basic loan support for portfolio analysis
- ✅ Unified reporting interface established
- ✅ Geographic/industry allocation framework

### Phase 2: PC Package Development (Future)
1. **Design Phase**:
   - Define detailed loan data model
   - Design integration points with amos_core
   - Plan advanced analytics requirements

2. **Implementation Phase**:
   - Build PC package models referencing amos_core
   - Implement advanced loan analytics
   - Create specialized credit reporting

3. **Integration Phase**:
   - Establish data pipelines between packages
   - Implement unified reporting views
   - Migrate advanced loan analysis to PC package

### Phase 3: Advanced Integration (Future)
1. **Cross-Package Analytics**:
   - Portfolio analysis across equity and detailed loan data
   - Risk analytics combining both asset classes
   - Unified performance attribution

2. **Specialized Reporting**:
   - Credit-specific dashboards and reports
   - Regulatory reporting combining both packages
   - Advanced portfolio optimization

## Benefits of This Architecture

### 1. Immediate Value
- **Unified Portfolio View**: Complete portfolio analysis across all instrument types
- **Consistent Interface**: Single query interface for all instruments
- **Basic Loan Support**: Essential loan analysis capabilities available now

### 2. Future Flexibility
- **Seamless Extension**: PC package adds capabilities without breaking existing functionality
- **Specialized Analytics**: Advanced credit analytics without complicating core model
- **Independent Development**: PC package can be developed and deployed independently

### 3. Operational Efficiency
- **Single Source of Truth**: Clear data ownership and responsibility
- **Reduced Complexity**: Core model remains simple and focused
- **Scalable Architecture**: Can support additional specialized packages (e.g., real estate, infrastructure)

## Best Practices for Development Teams

### For BI and Analytics Teams
1. **Start with amos_core**: Use unified instrument interface for all portfolio analysis
2. **Plan for PC Integration**: Design reports to accommodate future PC package data
3. **Leverage Unified Metrics**: Use instrument_snapshot for consistent performance metrics

### For Data Engineering Teams
1. **Maintain Clear Boundaries**: Keep amos_core focused on unified interface
2. **Design for Extension**: Ensure PC package can reference amos_core cleanly
3. **Preserve Referential Integrity**: Maintain data consistency across packages

### For Product Teams
1. **Unified User Experience**: Present single interface to users regardless of underlying complexity
2. **Progressive Enhancement**: Add PC package capabilities without disrupting existing workflows
3. **Clear Value Proposition**: Communicate benefits of unified vs. specialized analysis

This architecture provides a solid foundation for current needs while enabling future growth and specialization in private credit analytics.