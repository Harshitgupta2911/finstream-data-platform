{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (

    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        date_of_birth,
        country,
        created_at,
        updated_at,
        customer_status

    FROM {{ ref('stg_customers') }}

)

{% if is_incremental() %}

, existing_data AS (

    SELECT
        MAX(target.updated_at) AS max_updated_at
    FROM {{ this }} AS target

),

filtered_source AS (

    SELECT
        s.*

    FROM source_data AS s

    CROSS JOIN existing_data AS e

    WHERE s.updated_at >=
        e.max_updated_at - INTERVAL 1 DAY

)

SELECT *
FROM filtered_source

{% else %}

SELECT *
FROM source_data

{% endif %}