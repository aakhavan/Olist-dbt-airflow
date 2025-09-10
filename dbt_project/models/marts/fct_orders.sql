{{
    config(
        materialized='incremental',
        unique_key='order_id',
        -- Using delete+insert for simplicity in this project.
        -- For very large tables, 'merge' is often more performant.
        incremental_strategy='delete+insert'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_order_payments') }}
),

order_payments as (
    select
        order_id,
        sum(case when payment_type = 'credit_card' then payment_value else 0 end) as credit_card_amount,
        sum(case when payment_type = 'boleto' then payment_value else 0 end) as boleto_amount,
        sum(case when payment_type = 'voucher' then payment_value else 0 end) as voucher_amount,
        sum(case when payment_type = 'debit_card' then payment_value else 0 end) as debit_card_amount,
        sum(payment_value) as total_amount
    from payments
    group by 1
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        op.total_amount
    from orders o
    left join order_payments op on o.order_id = op.order_id
    where o.order_status = 'delivered'
    {% if is_incremental() %}
      and o.order_purchase_timestamp > (select max(order_purchase_timestamp) from {{ this }})
    {% endif %}
)

select * from final