select
    product_id,
    product_category_name,
    toInt64OrNull(product_name_lenght) as product_name_length,
    toInt64OrNull(product_description_lenght) as product_description_length,
    toInt64OrNull(product_photos_qty) as product_photos_qty,
    toFloat64OrNull(product_weight_g) as product_weight_g,
    toFloat64OrNull(product_length_cm) as product_length_cm,
    toFloat64OrNull(product_height_cm) as product_height_cm,
    toFloat64OrNull(product_width_cm) as product_width_cm

from {{ source('olist_raw', 'olist_products_dataset') }}