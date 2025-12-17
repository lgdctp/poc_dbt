{{ config(materialized='view') }}

select
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    paid_at
from (
    select
        payment_id::int as payment_id,
        order_id::int as order_id,
        lower(payment_method) as payment_method,
        lower(payment_status) as payment_status,
        amount::numeric(12,2) as amount,
        nullif(paid_at, '')::timestamp as paid_at
    from {{ ref('payments') }}
) raw
where payment_status in ('paid', 'pending', 'refunded', 'declined')
