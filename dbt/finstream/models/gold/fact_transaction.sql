with
    transactions as (select * from {{ ref("stg_transactions") }}),

    accounts as (select account_id, customer_id from {{ ref("dim_account") }}),

    dates as (select date_key, full_date from {{ ref("dim_date") }})

select

    t.transaction_id,

    -- Keys
    t.account_id,
    a.customer_id,
    t.merchant_id,
    d.date_key,

    -- Transaction details
    t.transaction_timestamp,
    t.transaction_date,
    t.transaction_type,
    t.amount,
    t.currency,
    t.transaction_status,
    t.payment_method,
    t.description

from transactions t

left join accounts a on t.account_id = a.account_id

left join dates d on t.transaction_date = d.full_date
