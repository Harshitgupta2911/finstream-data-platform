{{ config(
    materialized="incremental",
    unique_key="merchant_id",
    incremental_strategy="merge"
) }}

WITH source_data AS (

    SELECT
        merchant_id,
        merchant_name,
        merchant_category,
        country,
        city,
        currency,
        merchant_status,
        created_at,
        updated_at,
        status

    FROM {{ ref("stg_merchants") }}

)

{% if is_incremental() %}

, max_timestamp AS (

    SELECT
        COALESCE(
            MAX(updated_at),
            CAST('1900-01-01' AS TIMESTAMP)
        ) - INTERVAL 1 DAY AS cutoff_timestamp

    FROM {{ this }}

),

filtered_source AS (

    SELECT
        s.*

    FROM source_data AS s

    CROSS JOIN max_timestamp AS m

    WHERE s.updated_at >= m.cutoff_timestamp

)

SELECT *
FROM filtered_source

{% else %}

SELECT *
FROM source_data

{% endif %}