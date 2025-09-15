select
    order_id,
    payment_sequential,
    payment_type,
    
    round(cast(toFloat64(payment_value) as decimal(16, 2)), 2)
 as payment_value_dollars
from `raw`.`olist_order_payments_dataset`