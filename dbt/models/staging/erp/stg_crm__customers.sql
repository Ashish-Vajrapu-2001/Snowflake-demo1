-- View wrapper for snapshot to expose current records easily
{{ config(materialized='view') }}

SELECT
    CUSTOMER_ID,
    EMAIL,
    PHONE,
    FIRST_NAME,
    LAST_NAME,
    REGISTRATION_DATE,
    CUSTOMER_TYPE,
    STATUS,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at
FROM {{ ref('snp_customers') }}
WHERE dbt_is_current = TRUE
  AND (dbt_is_deleted = FALSE OR dbt_is_deleted IS NULL)