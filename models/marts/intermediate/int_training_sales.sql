{{ config(
    schema = 'dbt_stg'
)}}

with sales_demo as (
    select * from {{ ref('stg_training_sales_demo') }}
),

sales as (
    select 
        md5(cast(customer_id as varchar(50)) || '|' || cast(product_name as varchar(512)) || '|' || to_char(order_date,'yyyymmdd')) as order_key
      , customer_id
      , sale_customer_name
      , product_name
      , order_date
      , product_category
      , product
      , total_price 
    from sales_demo 
    where table_name is not null 
)

select * from sales 