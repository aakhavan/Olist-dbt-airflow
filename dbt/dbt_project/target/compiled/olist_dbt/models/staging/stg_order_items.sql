select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    parseDateTimeBestEffortOrNull(shipping_limit_date) as shipping_limit_date,
    toFloat64OrNull(price) as price,
    toFloat64OrNull(freight_value) as freight_value,
    
    round(cast(toFloat64(price) as decimal(16, 2)), 2)
 as price_dollars
from `raw`.`olist_order_items_dataset`