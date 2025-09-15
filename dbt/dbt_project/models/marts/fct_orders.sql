-- sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        order_by=('order_id', 'order_purchase_timestamp'),
        on_schema_change='ignore',
        settings={'allow_nullable_key': 1},
        schema='reporting'
    )
}}

with order_payments as (
    select
        order_id,
        sum(toFloat64(payment_value_dollars)) as total_amount
    from {{ ref('stg_order_payments') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.order_status,

    -- Always non-NULL DateTime for adapter's CAST('timestamp')
    ifNull(
      parseDateTimeBestEffortOrNull(nullIf(toString(o.order_purchase_timestamp), '')),
      toDateTime('1970-01-01 00:00:00')
    ) as order_purchase_timestamp,

    ifNull(
      parseDateTimeBestEffortOrNull(nullIf(toString(o.order_approved_at), '')),
      toDateTime('1970-01-01 00:00:00')
    ) as order_approved_at,

    ifNull(
      parseDateTimeBestEffortOrNull(nullIf(toString(o.order_delivered_customer_date), '')),
      toDateTime('1970-01-01 00:00:00')
    ) as order_delivered_customer_date,

    ifNull(op.total_amount, 0.0) as total_amount
from {{ ref('stg_orders') }} o
left join order_payments op on o.order_id = op.order_id

{% if is_incremental() %}
  where order_purchase_timestamp > (select max(order_purchase_timestamp) from {{ this }})
{% endif %}