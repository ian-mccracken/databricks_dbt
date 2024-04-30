{{ config(
    schema = 'edm'
)}}

with customers as (
    select * from {{ ref("int_training_customers") }}
),

sales as (
    select * from {{ ref('int_training_sales') }}
),

customer_sales as (
    select 
        md5(cast(coalesce(customers.customer_id,'') as varchar(50)) || '|' || cast(coalesce(sales.product_category,'') as varchar(100))) as dim_customer_key
      , customers.customer_id 
      , customers.customer_valid_from
      , customers.customer_valid_to
      , customers.customer_name
      , customers.loyalty_segment
      , customers.address1
      , customers.address2
      , customers.state
      , customers.city
      , customers.zip
      , customers.region
      , customers.district
      , sales.product_category
      , coalesce(count(sales.order_key),0) as order_ct 
      , coalesce(sum(sales.total_price),0.00) as total_sales
    from customers 
    left join sales 
        on customers.customer_id = sales.customer_id 
    where to_date(customers.customer_valid_to) = '9999-12-31'
    group by 
        customers.customer_id 
      , customers.customer_valid_from
      , customers.customer_valid_to
      , customers.customer_name
      , customers.loyalty_segment
      , customers.address1
      , customers.address2
      , customers.state
      , customers.city
      , customers.zip
      , customers.region
      , customers.district
      , customers.units_purchased
      , sales.product_category
)

select * from customer_sales