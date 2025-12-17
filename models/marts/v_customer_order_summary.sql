{{ config(materialized='view') }}

with orders as (
    select * from {{ ref('fct_orders') }}
),
customers as (
    select * from {{ ref('dim_customers') }}
)

select
    c.customer_id,
    c.full_name,
    c.state,
    c.customer_created_at,
    c.first_order_ts,
    c.last_order_ts,
    c.order_count,
    c.lifetime_value,
    sum(case when o.order_status = 'shipped' then 1 else 0 end) as shipped_orders,
    sum(case when o.order_status = 'cancelled' then 1 else 0 end) as cancelled_orders,
    sum(o.total_amount) as gross_revenue,
    sum(o.total_paid) as collected_revenue
from customers c
left join orders o
    on c.customer_id = o.customer_id
group by
    c.customer_id,
    c.full_name,
    c.state,
    c.customer_created_at,
    c.first_order_ts,
    c.last_order_ts,
    c.order_count,
    c.lifetime_value
