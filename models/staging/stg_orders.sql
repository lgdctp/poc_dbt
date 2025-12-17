{{ config(materialized='view') }}

with raw as (
    select
        order_id::int as order_id,
        customer_id::int as customer_id,
        order_date::timestamp as order_ts,
        lower(status) as status,
        total_amount::numeric(12,2) as total_amount
    from {{ ref('orders') }}
),
filtered as (
    select *
    from raw
    where status in ('shipped', 'processing', 'cancelled')
)

select
    order_id,
    customer_id,
    order_ts,
    status,
    total_amount
from filtered
