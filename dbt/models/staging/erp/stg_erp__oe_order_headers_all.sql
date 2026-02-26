{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ORDER_ID',
    merge_exclude_columns=['_silver_load_timestamp'],
    on_schema_change='append_new_columns',
    incremental_predicates=[
        "DBT_INTERNAL_DEST._bronze_sync_timestamp >= DATEADD('day', -{{ var('silver_lookback_days', 3) }}, CURRENT_TIMESTAMP())"
    ]
) }}

WITH source AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        TOTAL_AMOUNT,
        ORDER_STATUS,
        _FIVETRAN_SYNCED,
        _FIVETRAN_DELETED
    FROM {{ source('bronze_erp', 'OE_ORDER_HEADERS_ALL') }}

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

    {{ limit_dates_for_dev('ORDER_DATE') }}
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
            PARTITION BY ORDER_ID
            ORDER BY _FIVETRAN_SYNCED DESC
        ) AS _rn
    FROM active_records
),

transformed AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        TRY_TO_TIMESTAMP_NTZ(ORDER_DATE) AS ORDER_DATE,
        CAST(TOTAL_AMOUNT AS NUMBER(18,2)) AS TOTAL_AMOUNT,
        UPPER(TRIM(ORDER_STATUS)) AS ORDER_STATUS,

        _FIVETRAN_SYNCED          AS _bronze_sync_timestamp,
        CURRENT_TIMESTAMP()       AS _silver_load_timestamp,
        '{{ invocation_id }}'     AS _dbt_run_id,
        _FIVETRAN_DELETED         AS _is_deleted
    FROM deduped
    WHERE _rn = 1
)

SELECT * FROM transformed