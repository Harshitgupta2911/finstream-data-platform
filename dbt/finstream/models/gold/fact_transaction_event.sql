select
    e.event_id,
    e.transaction_id,
    e.event_date,
    e.event_timestamp,
    e.event_type,
    e.event_status,
    e.failure_reason,
    d.date_key

from {{ ref("stg_transaction_events") }} e

left join {{ ref("dim_date") }} d on e.event_date = d.full_date
