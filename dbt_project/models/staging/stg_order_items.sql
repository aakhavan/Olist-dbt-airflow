select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
from {{ source('olist_raw', 'olist_order_items_dataset') }}