{{ config(materialized = 'view') }}

with customers_normalized as (

    select
        customer_id::int       as customer_id,
        initcap(first_name)    as first_name,
        initcap(last_name)     as last_name,
        lower(email)           as email,
        created_at::timestamp  as created_at,
        upper(state)           as state
    from {{ ref('customers') }}

),

customers_dedup as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by created_at desc
        ) as rn
    from customers_normalized

)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    state,
    first_name || ' ' || last_name as full_name
from customers_dedup
where rn = 1
