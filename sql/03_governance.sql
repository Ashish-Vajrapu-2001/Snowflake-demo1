-- ============================================================
-- SECTION 1: PII classification tag
-- Created in BRONZE — shared tag used across all layers for lineage
-- ============================================================
CREATE TAG IF NOT EXISTS BRONZE.TAGS.PII_TYPE
    ALLOWED_VALUES
        'EMAIL', 'PHONE', 'FIRST_NAME', 'LAST_NAME', 'FULL_NAME',
        'DOB', 'ADDRESS', 'POSTCODE', 'NATIONAL_ID', 'FINANCIAL', 'OTHER';

-- ============================================================
-- SECTION 2: Masking policies
-- Created in GOLD — fire ONLY when consumers query Gold tables
-- Bronze and Silver: tags set for lineage; masking policy does NOT fire here
-- ============================================================
CREATE OR REPLACE MASKING POLICY GOLD.POLICIES.MASK_PII_STRING
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('COMPLIANCE_ROLE') THEN val
        WHEN IS_ROLE_IN_SESSION('ADMIN_ROLE')      THEN val
        ELSE '***MASKED***'
    END;

CREATE OR REPLACE MASKING POLICY GOLD.POLICIES.MASK_PII_DATE
    AS (val DATE) RETURNS DATE ->
    CASE
        WHEN IS_ROLE_IN_SESSION('COMPLIANCE_ROLE') THEN val
        WHEN IS_ROLE_IN_SESSION('ADMIN_ROLE')      THEN val
        ELSE DATE_FROM_PARTS(YEAR(val), 1, 1)
    END;

-- Link masking policies to PII tag (tag-based masking)
ALTER TAG BRONZE.TAGS.PII_TYPE
    SET MASKING POLICY GOLD.POLICIES.MASK_PII_STRING;

-- ============================================================
-- SECTION 3: Apply PII tags to Bronze columns
-- TAG ONLY — masking policy does NOT fire on Bronze (RULE G1)
-- ============================================================

-- BRONZE_CRM.Customers
ALTER TABLE BRONZE.BRONZE_CRM.Customers
    MODIFY COLUMN EMAIL
    SET TAG BRONZE.TAGS.PII_TYPE = 'EMAIL';

ALTER TABLE BRONZE.BRONZE_CRM.Customers
    MODIFY COLUMN PHONE
    SET TAG BRONZE.TAGS.PII_TYPE = 'PHONE';

ALTER TABLE BRONZE.BRONZE_CRM.Customers
    MODIFY COLUMN FIRST_NAME
    SET TAG BRONZE.TAGS.PII_TYPE = 'FIRST_NAME';

ALTER TABLE BRONZE.BRONZE_CRM.Customers
    MODIFY COLUMN LAST_NAME
    SET TAG BRONZE.TAGS.PII_TYPE = 'LAST_NAME';