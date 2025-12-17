{{ config(materialized='view') }}

with raw as (
    select
        customer_id::int as customer_id,
        initcap(first_name) as first_name,
        initcap(last_name) as last_name,
        lower(email) as email,
        created_at::timestamp as created_at,
        upper(state) as state
    from {{ ref('customers') }}
),
deduped as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        state,
        row_number() over (partition by customer_id order by created_at desc) as rn
    from raw
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    state,
    concat(first_name, ' ', last_name) as full_name
from deduped
where rn = 1
