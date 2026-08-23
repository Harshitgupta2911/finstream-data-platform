select
    account_id,
    customer_id,
    account_type,
    currency,
    balance,
    account_status,
    opened_at,
    cast(updated_at as timestamp) as updated_at

from {{ source("silver", "accounts") }}
