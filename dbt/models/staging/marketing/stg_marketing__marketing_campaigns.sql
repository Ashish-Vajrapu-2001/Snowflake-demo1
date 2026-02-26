{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='CAMPAIGN_ID',
    merge_exclude_columns=['_silver_load_timestamp'],
    on_schema_change='append_new_columns'
) }}

WITH source AS (
    SELECT
        CAMPAIGN_ID,
        CAMPAIGN_NAME,
        TOTAL_SPEND,
        CUSTOMERS_ACQUIRED,
        _FIVETRAN_SYNCED,
        _FIVETRAN_DELETED
    FROM {{ source('bronze_marketing', 'MARKETING_CAMPAIGNS') }}

    {% if is_incremental() %}
        WHERE _FIVETRAN_SYNCED > (
            SELECT COALESCE(
                MAX(_bronze_sync_timestamp),
                '{{ var("min_date", "2000-01-01") }}'::TIMESTAMP_TZ
            )
            FROM {{ this }}
        )
        OR _FIVETRAN_SYNCED >= DATEADD(
            'day',
            -{{ var('silver_lookback_days', 3) }},
            CURRENT_TIMESTAMP()
        )
    {% endif %}
),

active_records AS (
    SELECT *
    FROM source
    WHERE _FIVETRAN_DELETED = FALSE
       OR _FIVETRAN_DELETED IS NULL
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CAMPAIGN_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS _rn
    FROM active_records
),

transformed AS (
    SELECT
        CAMPAIGN_ID,
        CAST(CAMPAIGN_NAME AS VARCHAR) AS CAMPAIGN_NAME,
        CAST(TOTAL_SPEND AS NUMBER) AS TOTAL_SPEND,
        CAST(CUSTOMERS_ACQUIRED AS NUMBER) AS CUSTOMERS_ACQUIRED,

        _FIVETRAN_SYNCED          AS _bronze_sync_timestamp,
        CURRENT_TIMESTAMP()       AS _silver_load_timestamp,
        '{{ invocation_id }}'     AS _dbt_run_id,
        _FIVETRAN_DELETED         AS _is_deleted
    FROM deduped
    WHERE _rn = 1
)

SELECT * FROM transformed