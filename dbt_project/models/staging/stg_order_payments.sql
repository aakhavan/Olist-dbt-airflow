select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from {{ source('olist_raw', 'olist_order_payments_dataset') }}