

  create view `intermediate`.`stg_customers__dbt_tmp` 
  
    
    
  as (
    select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from `raw`.`olist_customers_dataset`
  )
      
      
                    -- end_of_sql
                    
                    