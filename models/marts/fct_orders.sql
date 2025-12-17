{{ config(
    materialized='table',
    dist='order_id',
    sort=['order_ts']
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),
payment_sums as (
    select
        order_id,
        sum(case when payment_status = 'paid' then amount else 0 end) as total_paid,
        sum(case when payment_status = 'refunded' then amount else 0 end) as total_refunded
    from {{ ref('stg_payments') }}
    group by order_id
),
payment_latest as (
    select
        order_id,
        payment_status as latest_payment_status,
        paid_at as last_paid_at,
        row_number() over (partition by order_id order by paid_at desc nulls last) as rn
    from {{ ref('stg_payments') }}
),
payments as (
    select
        coalesce(s.order_id, l.order_id) as order_id,
        s.total_paid,
        s.total_refunded,
        l.latest_payment_status,
        l.last_paid_at
    from payment_sums s
    full outer join (
        select order_id, latest_payment_status, last_paid_at
        from payment_latest
        where rn = 1
    ) l
    on s.order_id = l.order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_ts,
    o.status as order_status,
    o.total_amount,
    coalesce(p.total_paid, 0) as total_paid,
    coalesce(p.total_refunded, 0) as total_refunded,
    p.last_paid_at,
    p.latest_payment_status,
    case when coalesce(p.total_paid, 0) >= o.total_amount then 1 else 0 end as is_fully_paid
from orders o
left join payments p
    on o.order_id = p.order_id
