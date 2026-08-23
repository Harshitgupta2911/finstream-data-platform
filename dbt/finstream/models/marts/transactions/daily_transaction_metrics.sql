select
    d.full_date,
    d.year,
    d.month,
    d.month_name,

    count(t.transaction_id) as total_transactions,

    count(
        case when t.transaction_status = 'completed' then t.transaction_id end
    ) as successful_transactions,

    count(
        case when t.transaction_status = 'failed' then t.transaction_id end
    ) as failed_transactions,

    coalesce(sum(t.amount), 0) as total_transaction_value,

    coalesce(avg(t.amount), 0) as average_transaction_amount

from {{ ref("dim_date") }} d

left join {{ ref("fact_transaction") }} t on d.date_key = t.date_key

group by d.full_date, d.year, d.month, d.month_name
