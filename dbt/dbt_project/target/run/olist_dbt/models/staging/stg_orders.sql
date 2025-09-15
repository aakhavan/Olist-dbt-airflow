

  create view `intermediate`.`stg_orders__dbt_tmp` 
  
    
    
  as (
    select
    order_id,
    customer_id,
    order_status,
    parseDateTimeBestEffortOrNull(order_purchase_timestamp) as order_purchase_timestamp,
    parseDateTimeBestEffortOrNull(order_approved_at) as order_approved_at,
    parseDateTimeBestEffortOrNull(order_delivered_carrier_date) as order_delivered_carrier_date,
    parseDateTimeBestEffortOrNull(order_delivered_customer_date) as order_delivered_customer_date,
    parseDateTimeBestEffortOrNull(order_estimated_delivery_date) as order_estimated_delivery_date
from `raw`.`olist_orders_dataset`
  )
      
      
                    -- end_of_sql
                    
                    