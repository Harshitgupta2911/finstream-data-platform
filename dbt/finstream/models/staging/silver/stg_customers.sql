select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    country,
    created_at,
    cast(updated_at as timestamp) as updated_at,
    customer_status
from {{ source("silver", "customers") }}
