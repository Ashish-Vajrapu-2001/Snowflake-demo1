# Myntra CLV Analytics - Silver Layer

This dbt project transforms raw Bronze data (ingested via Fivetran) into a clean, conformed Silver layer in Snowflake.

## Architecture
*   **Source:** Bronze Tables (Fivetran)
*   **Transformation:** dbt Core (Incremental Merge & SCD2 Snapshots)
*   **Target:** Silver Tables (Snowflake)

## Models
*   **Staging (SCD1):** 11 incremental models handling deduplication and soft deletes.
*   **Snapshots (SCD2):** 2 snapshots tracking history for Customers and Items.

## Commands
*   `dbt run --selector silver_incremental`: Run standard incremental load.
*   `dbt snapshot --selector silver_snapshots`: Run snapshot updates.
*   `dbt test`: Run data quality tests.