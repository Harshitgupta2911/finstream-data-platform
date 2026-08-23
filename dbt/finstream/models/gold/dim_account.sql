{{
    config(
        materialized="incremental",
        unique_key="account_id",
        incremental_strategy="merge",
    )
}}

select
    account_id,
    customer_id,
    account_type,
    currency,
    balance,
    account_status,
    opened_at,
    updated_at

from {{ ref("stg_accounts") }}

{% if is_incremental() %}

WHERE updated_at >= (
    SELECT
        COALESCE(
            MAX(updated_at),
            CAST('1900-01-01' AS TIMESTAMP)
        ) - INTERVAL 1 DAY
    FROM {{ this }}
)

{% endif %}