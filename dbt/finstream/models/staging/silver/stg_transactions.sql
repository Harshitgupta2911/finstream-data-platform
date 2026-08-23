SELECT
    transaction_id,
    account_id,
    merchant_id,
    transaction_timestamp,
    DATE(transaction_timestamp) AS transaction_date,
    transaction_type,
    amount,
    currency,
    transaction_status,
    payment_method,
    description

FROM {{ source('silver', 'transactions') }}