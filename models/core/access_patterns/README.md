# Canonical Layer Access Control

## Overview

This directory contains security configurations and access control documentation for the canonical layer tables. The canonical layer provides secure, governed access to stable entity and relationship tables.

## Security Framework

### Row-Level Security (RLS)
The canonical layer implements fund-level row-level security to ensure users only access data for funds they are authorized to see.

### Column-Level Security
Sensitive columns are protected with data masking policies based on user roles and permissions.

### Access Control Principles

1. **Least Privilege**: Users get minimum access required for their role
2. **Fund-Level Isolation**: Multi-tenant security at the fund level
3. **Audit Trail**: Complete logging of all data access
4. **Data Classification**: Different security levels based on data sensitivity

## Security Roles

- `CANONICAL_READ`: Read access to canonical tables (with RLS filtering)
- `CANONICAL_ADMIN`: Full administrative access for data engineering
- `CANONICAL_AUDIT`: Audit access for compliance and monitoring

## Data Contracts

All canonical tables enforce strict data contracts through:
- Column-level data type enforcement
- Business rule validation
- Referential integrity constraints
- Data quality tests

## Usage Guidelines

1. Respect the security model - don't attempt to bypass RLS
2. Use the stable table contracts for reliable downstream consumption
3. Follow the documented entity relationships for joins
4. Implement proper error handling for security exceptions