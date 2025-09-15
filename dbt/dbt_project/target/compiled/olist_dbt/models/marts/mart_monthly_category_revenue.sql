with orders as (
    select order_id, order_purchase_timestamp from `reporting`.`fct_orders`
),

order_items as (
    select order_id, product_id, price from `intermediate`.`stg_order_items`
),

products as (
    select product_id, product_category_name from `intermediate`.`stg_products`
),

translations as (
    select * from `intermediate`.`stg_product_category_name_translation`
)

select
    toStartOfMonth(o.order_purchase_timestamp) as order_month,
    t.product_category_name_english as product_category,
    sum(oi.price) as total_revenue
from orders o
join order_items oi on o.order_id = oi.order_id
join products p on oi.product_id = p.product_id
left join translations t on p.product_category_name = t.product_category_name
where o.order_purchase_timestamp is not null and t.product_category_name_english is not null
group by 1, 2
order by 1, 3 desc