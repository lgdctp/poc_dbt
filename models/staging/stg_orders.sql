{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    order_ts,
    status,
    total_amount
from (
    select
        order_id::int as order_id,
        customer_id::int as customer_id,
        order_date::timestamp as order_ts,
        lower(status) as status,
        total_amount::numeric(12,2) as total_amount
    from {{ ref('orders') }}
) raw
where status in ('shipped', 'processing', 'cancelled')
