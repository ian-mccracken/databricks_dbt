{{ config(
    schema = 'dbt_stg'
)}}

with sales_demo as (
    select * from {{ ref('stg_training_sales_demo') }}
),

customers as (
    select 
         customer_id2 AS customer_id
       , tax_id
       , tax_code
       , customer_name
       , address1
       , address2
       , state
       , city
       , zip
       , region
       , district
       , longitude
       , latitude
       , ship_to_address
       , customer_valid_from
       , min(customer_valid_from) over(partition by customer_id2 order by customer_valid_from) as first_valid_timestamp
       , customer_valid_to
       , units_purchased
       , loyalty_segment 
    from sales_demo 
    where table_name1 is null
      and table_name is null
),

set_dates as (
    select 
         customer_id
       , tax_id
       , tax_code
       , customer_name
       , address1
       , address2
       , state
       , city
       , zip
       , region
       , district
       , longitude
       , latitude
       , ship_to_address
       , case when customer_valid_from = first_valid_timestamp then '1900-01-01 00:00:00.000'
              else customer_valid_from 
            end as customer_valid_from
       , coalesce(customer_valid_to, '9999-12-31 00:00:00.000') as customer_valid_to
       , units_purchased
       , loyalty_segment 
    from customers 
)

select * from set_dates