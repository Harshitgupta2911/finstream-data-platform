select
    merchant_id,
    merchant_name,
    merchant_category,
    country,
    city,
    currency,
    merchant_status,
    created_at,
    cast(updated_at as timestamp) as updated_at,
    status

from {{ source("silver", "merchants") }}
