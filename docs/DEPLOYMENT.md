# Myntra CLV Analytics — Bronze Layer Deployment Guide

## Prerequisites
- Snowflake account: {{PLACEHOLDER_SNOWFLAKE_ACCOUNT_ID}}
- Fivetran account with API access enabled
- Azure SQL: DBA access to run Change Tracking enablement on source DBs
- Python 3.8+ with requests and pyyaml libraries

## Deployment Steps

### Step 1 — Snowflake Infrastructure
  Run the setup script in Snowflake:
  `snowsql -a {{PLACEHOLDER_SNOWFLAKE_ACCOUNT_ID}} -f sql/01_setup_infrastructure.sql`
  
  **Verify:**
  - `SHOW DATABASES;` -> BRONZE, SILVER, GOLD
  - `SHOW WAREHOUSES;` -> LOADING_WH, TRANSFORM_WH
  - `SHOW ROLES;` -> LOADER_ROLE, TRANSFORMER_ROLE, ANALYST_ROLE

### Step 2 — Enable Change Tracking on Azure SQL Sources
  Run on each NATIVE_UPDATE source (requires DBA access on source):
  `sqlcmd -S {{PLACEHOLDER_SRC_001_HOST}} -d {{PLACEHOLDER_SRC_001_DB}} -i sql/02_enable_change_tracking.sql`
  
  **Verify:**
  - `SELECT name FROM sys.change_tracking_tables`
  
  *Note: Skip for Marketing source (TELEPORT) — Change Tracking not required.*

### Step 3 — Fivetran Setup (Group + Destination + Connectors)
  1. Fill in all `{{PLACEHOLDER_*}}` values in `fivetran/setup_fivetran.py`.
  2. Run the script:
     `python fivetran/setup_fivetran.py`
  
  **Verify:**
  - Check Fivetran dashboard → Connectors.
  - All 3 connectors (ERP, CRM, Marketing) should show "Connected" or "Syncing".

### Step 4 — Monitor Initial Sync
  Watch Fivetran dashboard → Connectors → Myntra_CLV_ERP_connector → Sync status.
  Bronze schemas (`BRONZE_ERP`, `BRONZE_CRM`, `BRONZE_MARKETING`) and tables are created automatically by Fivetran as the sync runs.

### Step 5 — Verify Bronze Tables Populated
  Run in Snowflake: