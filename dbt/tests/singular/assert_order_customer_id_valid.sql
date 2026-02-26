-- Assert: every CUSTOMER_ID in orders exists as an active record in customers snapshot
SELECT child.CUSTOMER_ID
FROM {{ ref('stg_erp__oe_order_headers_all') }} child
LEFT JOIN {{ ref('snp_customers') }} parent
    ON child.CUSTOMER_ID = parent.CUSTOMER_ID
    AND parent.dbt_is_current = TRUE
WHERE child.CUSTOMER_ID IS NOT NULL
  AND parent.CUSTOMER_ID IS NULL