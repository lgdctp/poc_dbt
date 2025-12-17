{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    state,
    concat(first_name, ' ', last_name) as full_name
from (
    select
        customer_id::int as customer_id,
        initcap(first_name) as first_name,
        initcap(last_name) as last_name,
        lower(email) as email,
        created_at::timestamp as created_at,
        upper(state) as state,
        row_number() over (partition by customer_id order by created_at desc) as rn
    from {{ ref('customers') }}
) raw
where rn = 1
