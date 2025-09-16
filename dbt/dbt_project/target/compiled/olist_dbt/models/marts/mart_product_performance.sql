with order_items as (
    select
        oi.order_id,
        oi.product_id,
        oi.price,
        oi.freight_value,
        o.order_purchase_timestamp
    from `intermediate`.`stg_order_items` oi
    join `intermediate`.`stg_orders` o on oi.order_id = o.order_id
),

order_reviews as (
    select
        order_id,
        avg(review_score) as avg_review_score
    from `intermediate`.`stg_order_reviews`
    group by 1
),

translations as (
    select * from `intermediate`.`stg_product_category_name_translation`
),

product_metrics_by_day as (
    select
        product_id,
        toDate(order_purchase_timestamp) as order_date,
        count(distinct order_id) as total_orders,
        sum(price) as total_revenue,
        count(order_id) as total_units_sold
    from order_items
    group by 1, 2
),

product_reviews as (
    select
        product_id,
        avg(avg_review_score) as average_review_score
    from order_items
    left join order_reviews using(order_id)
    group by product_id
)

select
    pm.product_id as product_id,
    pm.order_date as order_date,
    t.product_category_name_english as product_category,
    coalesce(pm.total_units_sold, 0) as total_units_sold,
    coalesce(pm.total_revenue, 0) as total_revenue,
    coalesce(pr.average_review_score, 0) as average_review_score
from product_metrics_by_day pm
left join `intermediate`.`stg_products` p on p.product_id = pm.product_id
left join product_reviews pr on pm.product_id = pr.product_id
left join translations t on p.product_category_name = t.product_category_name
where pm.total_orders is not null
order by 2,3