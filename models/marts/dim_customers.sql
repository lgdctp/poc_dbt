{{ config(
    materialized='table',
    dist='customer_id',
    sort=['created_at']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),
orders_summary as (
    select
        customer_id,
        min(order_ts) as first_order_ts,
        max(order_ts) as last_order_ts,
        count(*) as order_count,
        sum(total_amount) as lifetime_value
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.state,
    c.created_at as customer_created_at,
    o.first_order_ts,
    o.last_order_ts,
    coalesce(o.order_count, 0) as order_count,
    coalesce(o.lifetime_value, 0) as lifetime_value
from customers c
left join orders_summary o
    on c.customer_id = o.customer_id
