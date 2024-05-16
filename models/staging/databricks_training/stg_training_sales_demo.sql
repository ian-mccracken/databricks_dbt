{{ config(
    schema = 'dbt_stg'
)}}

with source as (
    select 
        * 
    from {{ source('training','sales_demo') }}
),

renamed as (
    select 
        table_name 
      , customer_id 
      , customer_name as sale_customer_name
      , cast(product_name as varchar(512)) as product_name
      , order_date
      , cast(product_category as varchar(100)) as product_category 
      , product
      , cast(total_price as decimal(19,2)) as total_price
      , table_name1
      , first_timestamp
      , gym
      , last_timestamp
      , mac
      , customer_id2
      , tax_id
      , tax_code
      , customer_name3 as customer_name
      , cast(number as varchar(10)) || ' ' || cast(street as varchar(256)) as address1 
      , unit as address2
      , city
      , state
      , postcode as zip
      , region 
      , cast(district as varchar(256)) as district
      , lon as longitude
      , lat as latitude 
      , ship_to_address
      , to_timestamp(valid_from) as customer_valid_from
      , to_timestamp(valid_to) as customer_valid_to
      , units_purchased
      , loyalty_segment
    from source 
)

select * from renamed