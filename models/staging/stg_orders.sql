{{ config(materialized = 'view') }}

with orders_normalized as (

    select
        order_id::int        as order_id,
        customer_id::int     as customer_id,
        order_date::timestamp as order_date,
        status,
        total_amount::numeric(18,2) as total_amount
    from {{ ref('orders') }}

),

orders_dedup as (

    select
        *,
        row_number() over (
            partition by order_id
            order by order_date desc
        ) as rn
    from orders_normalized

)

select
    order_id,
    customer_id,
    order_date,
    status,
    total_amount
from orders_dedup
where rn = 1
