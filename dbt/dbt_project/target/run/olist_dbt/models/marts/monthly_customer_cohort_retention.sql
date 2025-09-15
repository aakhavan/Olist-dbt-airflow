
  
    
    
    
        
        insert into `reporting`.`monthly_customer_cohort_retention`
        ("cohort_month", "month_number", "total_customers", "active_customers", "retention_rate")-- dbt/dbt_project/models/marts/monthly_customer_cohort_retention.sql


with orders as (
    select
        customer_id,
        toDate(order_purchase_timestamp) as order_date,
        toStartOfMonth(toDate(order_purchase_timestamp)) as order_month
    from `reporting`.`fct_orders`
    where customer_id is not null
),

customer_cohorts as (
    select
        customer_id,
        min(order_date) as first_order_date,
        toStartOfMonth(min(order_date)) as cohort_month
    from orders
    group by customer_id
),

subsequent_orders as (
    select distinct
        customer_id,
        order_month
    from orders
),

cohort_activity as (
    select
        c.customer_id,
        c.cohort_month,
        s.order_month,
        dateDiff('month', c.cohort_month, s.order_month) as month_number
    from customer_cohorts c
    join subsequent_orders s 
      on c.customer_id = s.customer_id
    where dateDiff('month', c.cohort_month, s.order_month) >= 0
),

cohort_size as (
    select
        cohort_month,
        countDistinct(customer_id) as total_customers
    from customer_cohorts
    group by cohort_month
),

monthly_active_customers as (
    select
        cohort_month,
        month_number,
        countDistinct(customer_id) as active_customers
    from cohort_activity
    group by cohort_month, month_number
)

select
    m.cohort_month,
    m.month_number,
    s.total_customers,
    m.active_customers,
    if(s.total_customers > 0, round(m.active_customers * 100.0 / s.total_customers, 2), null) as retention_rate
from monthly_active_customers m
join cohort_size s
  on m.cohort_month = s.cohort_month
order by
    m.cohort_month, m.month_number
  