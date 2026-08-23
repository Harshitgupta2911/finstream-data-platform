select
    m.merchant_id,
    m.merchant_name,
    m.merchant_category,
    m.country,
    m.city,
    m.merchant_status,

    count(t.transaction_id) as total_transactions,

    count(
        case when t.transaction_status = 'completed' then t.transaction_id end
    ) as successful_transactions,

    count(
        case when t.transaction_status = 'failed' then t.transaction_id end
    ) as failed_transactions,

    coalesce(sum(t.amount), 0) as total_transaction_value,

    coalesce(avg(t.amount), 0) as average_transaction_amount

from {{ ref("dim_merchant") }} m

left join {{ ref("fact_transaction") }} t on m.merchant_id = t.merchant_id

group by
    m.merchant_id,
    m.merchant_name,
    m.merchant_category,
    m.country,
    m.city,
    m.merchant_status
