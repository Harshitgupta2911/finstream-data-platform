SELECT
    event_id,
    transaction_id,
    event_type,
    event_timestamp,
    DATE(event_timestamp) AS event_date,
    event_status,
    failure_reason

FROM {{ source('silver', 'transaction_events') }}