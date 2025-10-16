-- Security Policies for AMOS Canonical Data Layer
-- Row-level and column-level security configurations

{% macro create_row_level_security_policy(table_name, policy_name, filter_condition) %}
  -- Create row-level security policy for multi-tenant access
  {% set sql %}
    CREATE OR REPLACE ROW ACCESS POLICY {{ policy_name }}
    AS ({{ filter_condition }})
  {% endset %}
  
  {{ log("Creating RLS policy: " ~ policy_name ~ " for table: " ~ table_name, info=True) }}
  {{ return(sql) }}
{% endmacro %}

{% macro apply_fund_level_security(table_name, fund_column='fund_id') %}
  -- Apply fund-level row security based on user context
  {% set policy_name = table_name ~ '_fund_access_policy' %}
  {% set filter_condition %}
    {{ fund_column }} IN (
      SELECT fund_id 
      FROM user_fund_access 
      WHERE user_name = CURRENT_USER()
        AND access_type IN ('READ', 'FULL')
        AND (expiry_date IS NULL OR expiry_date >= CURRENT_DATE())
    )
  {% endset %}
  
  {{ return(create_row_level_security_policy(table_name, policy_name, filter_condition)) }}
{% endmacro %}

{% macro create_investor_data_masking() %}
  -- Create data masking policies for sensitive investor information
  {% set policies = [] %}
  
  -- Mask investor names for non-authorized users
  {% set investor_name_policy %}
    CREATE OR REPLACE MASKING POLICY investor_name_mask AS (val STRING) 
    RETURNS STRING ->
    CASE 
      WHEN CURRENT_ROLE() IN ('INVESTOR_RELATIONS', 'FUND_ADMIN', 'COMPLIANCE_OFFICER') 
        THEN val
      WHEN CURRENT_ROLE() IN ('ANALYST', 'PORTFOLIO_MANAGER')
        THEN 'Investor_' || RIGHT(SHA2(val), 8)
      ELSE '***MASKED***'
    END
  {% endset %}
  {% do policies.append(investor_name_policy) %}
  
  -- Mask financial amounts for unauthorized users
  {% set financial_amount_policy %}
    CREATE OR REPLACE MASKING POLICY financial_amount_mask AS (val NUMBER) 
    RETURNS NUMBER ->
    CASE 
      WHEN CURRENT_ROLE() IN ('FUND_ADMIN', 'FINANCE_TEAM', 'COMPLIANCE_OFFICER') 
        THEN val
      WHEN CURRENT_ROLE() IN ('ANALYST', 'PORTFOLIO_MANAGER')
        THEN ROUND(val / 1000000, 1)  -- Show in millions, rounded
      ELSE NULL
    END
  {% endset %}
  {% do policies.append(financial_amount_policy) %}
  
  -- Mask contact information
  {% set contact_info_policy %}
    CREATE OR REPLACE MASKING POLICY contact_info_mask AS (val STRING) 
    RETURNS STRING ->
    CASE 
      WHEN CURRENT_ROLE() IN ('INVESTOR_RELATIONS', 'COMPLIANCE_OFFICER') 
        THEN val
      ELSE '***MASKED***'
    END
  {% endset %}
  {% do policies.append(contact_info_policy) %}
  
  {{ return(policies) }}
{% endmacro %}

{% macro apply_column_level_security() %}
  -- Apply column-level security policies to sensitive columns
  {% set security_applications = [] %}
  
  -- Apply investor name masking
  {% set investor_tables = ['investor', 'investor_commitment_summary'] %}
  {% for table in investor_tables %}
    {% set apply_policy %}
      ALTER TABLE {{ ref(table) }} 
      MODIFY COLUMN name 
      SET MASKING POLICY investor_name_mask
    {% endset %}
    {% do security_applications.append(apply_policy) %}
  {% endfor %}
  
  -- Apply financial amount masking to sensitive financial columns
  {% set financial_columns = [
    {'table': 'fund_snapshot', 'columns': ['total_nav', 'total_commitment', 'total_called', 'total_distributed']},
    {'table': 'investment_snapshot', 'columns': ['nav', 'cost_basis', 'unrealized_gain_loss']},
    {'table': 'loan_snapshot', 'columns': ['outstanding_principal', 'total_exposure', 'provision_amount']},
    {'table': 'transaction', 'columns': ['amount']},
    {'table': 'loan_cashflow', 'columns': ['principal_amount', 'interest_amount', 'total_amount']}
  ] %}
  
  {% for table_config in financial_columns %}
    {% for column in table_config.columns %}
      {% set apply_policy %}
        ALTER TABLE {{ ref(table_config.table) }} 
        MODIFY COLUMN {{ column }} 
        SET MASKING POLICY financial_amount_mask
      {% endset %}
      {% do security_applications.append(apply_policy) %}
    {% endfor %}
  {% endfor %}
  
  {{ return(security_applications) }}
{% endmacro %}

{% macro create_user_access_control_table() %}
  -- Create user access control table for managing permissions
  {% set sql %}
    CREATE TABLE IF NOT EXISTS user_fund_access (
      user_name VARCHAR(255) NOT NULL,
      fund_id UUID NOT NULL,
      access_type VARCHAR(20) NOT NULL, -- 'READ', 'WRITE', 'FULL'
      granted_by VARCHAR(255) NOT NULL,
      granted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
      expiry_date DATE,
      reason VARCHAR(500),
      PRIMARY KEY (user_name, fund_id),
      FOREIGN KEY (fund_id) REFERENCES {{ ref('fund') }}(id)
    );
    
    CREATE TABLE IF NOT EXISTS user_role_permissions (
      role_name VARCHAR(100) NOT NULL,
      table_name VARCHAR(100) NOT NULL,
      permission_type VARCHAR(20) NOT NULL, -- 'SELECT', 'INSERT', 'UPDATE', 'DELETE'
      column_restrictions TEXT, -- JSON array of restricted columns
      row_filter TEXT, -- SQL condition for row-level filtering
      created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
      updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY (role_name, table_name, permission_type)
    );
  {% endset %}
  
  {{ return(sql) }}
{% endmacro %}

{% macro setup_bi_user_roles() %}
  -- Setup standard BI user roles with appropriate permissions
  {% set role_configs = [
    {
      'role': 'BI_ANALYST',
      'description': 'Standard BI analyst with read access to most data',
      'permissions': ['SELECT'],
      'restricted_columns': ['investor.name', 'counterparty.name'],
      'fund_access': 'ALL'
    },
    {
      'role': 'PORTFOLIO_MANAGER', 
      'description': 'Portfolio managers with full investment data access',
      'permissions': ['SELECT'],
      'restricted_columns': ['investor.name'],
      'fund_access': 'ASSIGNED_FUNDS'
    },
    {
      'role': 'INVESTOR_RELATIONS',
      'description': 'Investor relations team with full investor data access',
      'permissions': ['SELECT'],
      'restricted_columns': [],
      'fund_access': 'ALL'
    },
    {
      'role': 'COMPLIANCE_OFFICER',
      'description': 'Compliance officers with full access for regulatory reporting',
      'permissions': ['SELECT'],
      'restricted_columns': [],
      'fund_access': 'ALL'
    },
    {
      'role': 'EXTERNAL_AUDITOR',
      'description': 'External auditors with limited access to financial data',
      'permissions': ['SELECT'],
      'restricted_columns': ['investor.name', 'counterparty.name'],
      'fund_access': 'AUDIT_SCOPE'
    }
  ] %}
  
  {% set setup_commands = [] %}
  
  {% for role_config in role_configs %}
    {% set create_role %}
      CREATE ROLE IF NOT EXISTS {{ role_config.role }};
      COMMENT ON ROLE {{ role_config.role }} IS '{{ role_config.description }}';
    {% endset %}
    {% do setup_commands.append(create_role) %}
    
    -- Grant permissions to canonical tables
    {% set canonical_tables = [
      'fund', 'company', 'investor', 'counterparty',
      'commitment', 'investment', 'facility', 'loan', 'opportunity',
      'fund_snapshot', 'investment_snapshot', 'loan_snapshot',
      'transaction', 'loan_cashflow',
      'currency', 'country', 'industry', 'investor_type', 'stage'
    ] %}
    
    {% for table in canonical_tables %}
      {% set grant_permission %}
        GRANT SELECT ON {{ ref(table) }} TO ROLE {{ role_config.role }};
      {% endset %}
      {% do setup_commands.append(grant_permission) %}
    {% endfor %}
    
    -- Grant permissions to BI views
    {% set bi_views = [
      'fund_performance_summary',
      'portfolio_company_summary', 
      'credit_portfolio_summary',
      'investor_commitment_summary'
    ] %}
    
    {% for view in bi_views %}
      {% set grant_view_permission %}
        GRANT SELECT ON {{ ref(view) }} TO ROLE {{ role_config.role }};
      {% endset %}
      {% do setup_commands.append(grant_view_permission) %}
    {% endfor %}
  {% endfor %}
  
  {{ return(setup_commands) }}
{% endmacro %}

{% macro create_audit_logging() %}
  -- Create audit logging for sensitive data access
  {% set sql %}
    CREATE TABLE IF NOT EXISTS data_access_audit (
      audit_id UUID DEFAULT UUID_GENERATE_V4(),
      user_name VARCHAR(255) NOT NULL,
      role_name VARCHAR(100),
      table_name VARCHAR(100) NOT NULL,
      operation VARCHAR(20) NOT NULL, -- 'SELECT', 'INSERT', 'UPDATE', 'DELETE'
      row_count INTEGER,
      columns_accessed TEXT, -- JSON array of column names
      filter_conditions TEXT, -- SQL WHERE conditions used
      access_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
      session_id VARCHAR(255),
      client_ip VARCHAR(45),
      application_name VARCHAR(255),
      query_id VARCHAR(255),
      PRIMARY KEY (audit_id)
    );
    
    -- Create indexes for efficient audit queries
    CREATE INDEX IF NOT EXISTS idx_audit_user_timestamp 
    ON data_access_audit (user_name, access_timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_audit_table_timestamp 
    ON data_access_audit (table_name, access_timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_audit_operation_timestamp 
    ON data_access_audit (operation, access_timestamp);
  {% endset %}
  
  {{ return(sql) }}
{% endmacro %}

{% macro generate_security_documentation() %}
  -- Generate security documentation for compliance
  {% set doc_content %}
    # AMOS Data Security Framework
    
    ## Row-Level Security (RLS)
    - Fund-level access control based on user assignments
    - Automatic filtering of data based on user permissions
    - Configurable access expiry dates
    
    ## Column-Level Security
    - Data masking for sensitive information (investor names, financial amounts)
    - Role-based access to different levels of detail
    - Contact information protection
    
    ## User Roles and Permissions
    - BI_ANALYST: Standard analytical access with masked sensitive data
    - PORTFOLIO_MANAGER: Full investment data access for assigned funds
    - INVESTOR_RELATIONS: Full investor data access
    - COMPLIANCE_OFFICER: Complete access for regulatory compliance
    - EXTERNAL_AUDITOR: Limited access for audit purposes
    
    ## Audit and Compliance
    - Complete audit trail of data access
    - Query logging and monitoring
    - Regular access reviews and certifications
    
    ## Data Classification
    - PUBLIC: General reference data (currencies, countries)
    - INTERNAL: Fund and investment performance data
    - CONFIDENTIAL: Investor information and detailed financials
    - RESTRICTED: Sensitive counterparty and transaction details
  {% endset %}
  
  {{ return(doc_content) }}
{% endmacro %}