select exchange_rate_id, base_currency, target_currency, rate, rate_date, source

from {{ ref("stg_exchange_rates") }}
