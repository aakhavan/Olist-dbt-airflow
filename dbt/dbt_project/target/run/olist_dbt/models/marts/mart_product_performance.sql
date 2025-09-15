
  
    
    
    
        
        insert into `reporting`.`mart_product_performance__dbt_backup`
        ("product_id", "product_category", "total_units_sold", "total_revenue", "average_review_score")with order_items as (
    select
        order_id,
        product_id,
        price,
        freight_value
    from `intermediate`.`stg_order_items`
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


product_metrics as (
    select
        product_id,
        count(distinct order_id) as total_orders,
        sum(price) as total_revenue,
        sum(freight_value) as total_freight_cost,
        count(order_id) as total_units_sold
    from order_items
    group by 1
),

-- Aggregate review scores, joining through order_items to link reviews to products
product_reviews as (
    select
        product_id,
        avg(avg_review_score) as average_review_score
    from order_items
    left join order_reviews using(order_id)
    group by product_id
)

select
    p.product_id as product_id,
    t.product_category_name_english as product_category,
    coalesce(pm.total_units_sold, 0) as total_units_sold,
    coalesce(pm.total_revenue, 0) as total_revenue,
    coalesce(pr.average_review_score, 0) as average_review_score
from `intermediate`.`stg_products` p
left join product_metrics pm on p.product_id = pm.product_id
left join product_reviews pr on p.product_id = pr.product_id
left join translations t on p.product_category_name = t.product_category_name
where pm.total_orders is not null -- Only include products that have been sold
order by total_revenue desc
  