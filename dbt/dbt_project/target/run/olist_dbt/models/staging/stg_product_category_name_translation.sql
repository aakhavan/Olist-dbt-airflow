

  create view `intermediate`.`stg_product_category_name_translation__dbt_tmp` 
  
    
    
  as (
    select
    product_category_name,
    product_category_name_english

from `raw`.`product_category_name_translation`
  )
      
      
                    -- end_of_sql
                    
                    