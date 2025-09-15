{{
    config(
        materialized='table',
        schema='reporting',
        order_by=('customer_unique_id',),
        settings={'allow_nullable_key': 1}
    )
}}

select
    assumeNotNull(c.customer_unique_id) as customer_unique_id ,
    c.customer_state,
    min(o.order_purchase_timestamp) as first_order_date,
    max(o.order_purchase_timestamp) as last_order_date,
    count(o.order_id) as number_of_orders,
    sum(o.total_amount) as total_revenue
from {{ ref('fct_orders') }} o
join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
group by 1, 2
order by total_revenue desc