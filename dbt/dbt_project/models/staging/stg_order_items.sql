select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    parseDateTimeBestEffortOrNull(shipping_limit_date) as shipping_limit_date,
    toFloat64OrNull(price) as price,
    toFloat64OrNull(freight_value) as freight_value,
    {{ to_dollars('price') }} as price_dollars
from {{ source('olist_raw', 'olist_order_items_dataset') }}