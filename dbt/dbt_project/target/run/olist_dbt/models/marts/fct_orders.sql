
        insert into `reporting`.`fct_orders`
        ("order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_customer_date", "total_amount")-- sql


with order_payments as (
    select
        order_id,
        sum(toFloat64(payment_value_dollars)) as total_amount
    from `intermediate`.`stg_order_payments`
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
from `intermediate`.`stg_orders` o
left join order_payments op on o.order_id = op.order_id


  where order_purchase_timestamp > (select max(order_purchase_timestamp) from `reporting`.`fct_orders`)

    