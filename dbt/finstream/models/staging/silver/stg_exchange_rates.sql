SELECT
    exchange_rate_id,
    base_currency,
    target_currency,
    rate,
    rate_date,
    source

FROM {{ source('silver', 'exchange_rates') }}