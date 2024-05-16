
with 

source as (
    
    select * from {{ source('jaffle_shop','jaffle_shop_stores') }} 

),

renamed as (

    select 

        ----------  ids
        id as location_id,

        ----------  properties 
        name as location_name,
        tax_rate,

        ----------  timestamps 
        opened_at 

    from source 

)

select * from renamed;