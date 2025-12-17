{{ config(materialized='view') }}

with payments as (
    select * from {{ ref('stg_payments') }}
)

select
    payment_method,
    payment_status,
    count(*) as payment_count,
    sum(amount) as total_amount,
    max(paid_at) as last_event_at
from payments
group by payment_method, payment_status
