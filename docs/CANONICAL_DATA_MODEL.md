# AMOS Canonical Data Model

## Overview

The AMOS canonical data layer provides clean, validated, and business-ready entity and relationship tables with stable contracts that downstream consumers can rely on. This layer serves as the single source of truth for all business entities in your organization.

## Design Principles

### 1. Stable Contracts
- All tables have enforced data contracts with strict column definitions
- Schema changes follow a formal change management process
- Backward compatibility is maintained for downstream consumers

### 2. Business Entity Focus
- Tables represent core business entities and their relationships
- Each entity has a clear business purpose and ownership
- Relationships between entities are explicitly modeled

### 3. Data Quality Assurance
- Comprehensive data quality tests ensure reliability
- Business rule validation at the canonical layer
- Automated monitoring and alerting for data issues

### 4. Security and Governance
- Row-level security for multi-tenant access
- Column-level security for sensitive data
- Complete audit trail and data lineage

## Entity Relationship Model

### Core Business Entities

#### Fund
- **Purpose**: Investment funds managed by the organization
- **Key Attributes**: Name, vintage, strategy, fee structure, target size
- **Relationships**: One-to-many with investments, commitments, facilities

#### Company  
- **Purpose**: Portfolio companies and borrowers
- **Key Attributes**: Name, industry classification, geographic exposure
- **Relationships**: Many-to-many with industries/countries via bridge tables

#### Investor
- **Purpose**: Limited partners and fund investors
- **Key Attributes**: Name, type, KYC classification
- **Relationships**: One-to-many with commitments

#### Counterparty
- **Purpose**: External parties (lenders, service providers, etc.)
- **Key Attributes**: Name, type, jurisdiction
- **Relationships**: Many-to-many with facilities via syndicate structure

### Relationship Entities

#### Investment
- **Purpose**: Fund investments in portfolio companies
- **Key Attributes**: Investment type, description
- **Relationships**: Links funds to companies

#### Commitment  
- **Purpose**: Investor commitments to funds
- **Key Attributes**: Commitment terms and conditions
- **Relationships**: Links investors to funds

#### Facility
- **Purpose**: Lending facilities to borrowers
- **Key Attributes**: Facility type, size, terms, maturity
- **Relationships**: Links funds to borrowing companies

#### Loan
- **Purpose**: Individual loans within facilities
- **Key Attributes**: Loan type, amount, interest terms
- **Relationships**: Child of facility

### Bridge Entities (Many-to-Many)

#### Geographic Allocations
- **company_country**: Company geographic exposure
- **loan_country**: Loan geographic exposure (time-variant)
- **facility_country**: Facility geographic exposure (time-variant)

#### Industry Allocations
- **company_industry**: Company industry exposure  
- **loan_industry**: Loan industry exposure (time-variant)
- **facility_industry**: Facility industry exposure (time-variant)

#### Syndicate Structure
- **facility_lender**: Lender participation in facilities

### Snapshots and Transactions

#### Performance Snapshots
- **fund_snapshot**: Daily fund performance metrics
- **instrument_snapshot**: Daily instrument valuations (unified equity and loan snapshots)
- **loan_snapshot**: Daily loan positions and risk metrics

#### Transaction Records
- **transaction**: All financial transactions
- **loan_cashflow**: Detailed loan payment tracking

### Supporting Entities

#### Equity Details
- **share_class**: Equity share class definitions
- **shareholder**: Ownership tracking
- **investment_round**: Funding round details

#### Valuation and Forecasting
- **company_valuation**: Valuation history
- **company_dividend_forecast**: Dividend projections
- **valuation_policy**: Valuation methodologies

#### Interest and Calculations
- **loan_interest_period**: Interest calculation periods

## Data Contracts and Schema Enforcement

### Contract Enforcement
All canonical tables use dbt contracts with:
- **Data Type Validation**: Strict column data types
- **Constraint Enforcement**: NOT NULL, PRIMARY KEY, FOREIGN KEY constraints
- **Business Rule Validation**: Custom tests for business logic

### Schema Stability
- **Additive Changes Only**: New columns can be added, existing columns cannot be removed
- **Data Type Consistency**: Data types cannot be changed without migration
- **Naming Conventions**: Consistent naming across all tables

### Version Management
- **Semantic Versioning**: Major.Minor.Patch versioning for schema changes
- **Change Documentation**: All changes documented with business justification
- **Migration Scripts**: Automated migration for schema updates

## Data Quality Framework

### Test Categories

#### Critical Tests (Must Pass)
- Primary key uniqueness
- Foreign key referential integrity  
- Business metric calculations (DPI, RVPI, etc.)
- Data type and constraint validation

#### Business Rule Tests
- Allocation percentages sum to 100%
- Performance metric formulas
- Date logic validation
- Amount and percentage ranges

#### Freshness Tests
- Snapshot data updated within SLA
- Transaction data near real-time
- Master data refresh schedules

#### Volume Tests
- Anomaly detection for unusual patterns
- Record count validation
- Data completeness monitoring

### Monitoring and Alerting
- **Real-time Monitoring**: Critical test failures trigger immediate alerts
- **Daily Reports**: Summary of all test results
- **Trend Analysis**: Data quality metrics over time
- **Root Cause Analysis**: Automated investigation of failures

## Security Model

### Row-Level Security (RLS)
- **Fund-Level Access**: Users see only authorized fund data
- **Automatic Filtering**: Security applied transparently
- **Access Management**: Centralized permission management

### Column-Level Security
- **Data Masking**: Sensitive data masked based on user roles
- **Financial Data**: Amount aggregation for external users
- **PII Protection**: Investor and counterparty name masking

### Audit and Compliance
- **Access Logging**: Complete audit trail of data access
- **Change Tracking**: All data changes logged with user context
- **Compliance Reporting**: Automated compliance reports

## Usage Guidelines for Downstream Consumers

### APIs
- Use stable table contracts for API development
- Implement proper error handling for data quality issues
- Respect security boundaries and access controls

### Marts and Analytics
- Build on canonical tables as the foundation
- Use documented entity relationships for joins
- Implement additional business logic in mart layer

### External Systems
- Connect through secure, authenticated channels
- Use read-only access patterns
- Implement proper retry logic for transient failures

### Data Science and ML
- Use canonical data as training data source
- Respect data governance and privacy requirements
- Document feature engineering and transformations

## Performance Considerations

### Clustering Strategy
- **fund_id**: Primary clustering key for multi-tenant performance
- **as_of_date**: Secondary clustering for time-series queries
- **created_at/updated_at**: For incremental processing

### Materialization Strategy
- **Tables**: Dimensions and master data for fast lookups
- **Incremental**: Large fact tables (snapshots, transactions)
- **Views**: Simple transformations and security layers

### Query Optimization
- Use clustered columns in WHERE clauses
- Limit date ranges for snapshot queries
- Leverage foreign key relationships for joins

## Change Management Process

### Schema Changes
1. **Proposal**: Document business need and impact analysis
2. **Review**: Technical and business stakeholder approval
3. **Testing**: Validate changes in development environment
4. **Migration**: Automated deployment with rollback capability
5. **Communication**: Notify all downstream consumers

### Data Model Evolution
- **Backward Compatibility**: Maintain existing interfaces
- **Deprecation Process**: Formal sunset timeline for old structures
- **Migration Support**: Tools and documentation for consumers

## Support and Governance

### Data Stewardship
- **Entity Owners**: Business owners for each entity type
- **Technical Owners**: Data engineering team for implementation
- **Quality Owners**: Data quality team for monitoring

### Documentation Maintenance
- **Schema Documentation**: Automatically generated from dbt models
- **Business Context**: Maintained by business stakeholders
- **Technical Details**: Maintained by data engineering team

### Issue Resolution
- **Data Quality Issues**: Automated detection and alerting
- **Schema Issues**: Formal change request process
- **Performance Issues**: Monitoring and optimization

This canonical data model provides a stable, well-governed foundation that any downstream consumer can rely on with confidence.