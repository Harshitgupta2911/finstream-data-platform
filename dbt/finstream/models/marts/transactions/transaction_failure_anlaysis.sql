select
    e.event_type,
    e.failure_reason,

    count(e.event_id) as total_events,

    count(distinct e.transaction_id) as affected_transactions

from {{ ref("fact_transaction_event") }} e

where e.event_type = 'failed'

group by e.event_type, e.failure_reason
