select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.customer_status,

    count(t.transaction_id) as total_transactions,

    count(
        case when t.transaction_status = 'completed' then t.transaction_id end
    ) as successful_transactions,

    count(
        case when t.transaction_status = 'failed' then t.transaction_id end
    ) as failed_transactions,

    coalesce(sum(t.amount), 0) as total_transaction_value,

    coalesce(avg(t.amount), 0) as average_transaction_amount,

    max(t.transaction_date) as last_transaction_date

from {{ ref("dim_customer") }} c

left join {{ ref("fact_transaction") }} t on c.customer_id = t.customer_id

group by c.customer_id, c.first_name, c.last_name, c.country, c.customer_status
