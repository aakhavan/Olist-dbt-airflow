select
    order_id,
    payment_sequential,
    payment_type,
    {{ to_dollars('payment_value') }} as payment_value_dollars
from {{ source('olist_raw', 'olist_order_payments_dataset') }}