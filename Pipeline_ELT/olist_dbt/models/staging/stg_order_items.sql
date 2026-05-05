with source as (

    select *
    from {{ source('olist_raw', 'raw_order_items') }}

),

renamed as (

    select
        cast(order_id as varchar) as order_id,
        cast(order_item_id as integer) as order_item_id,
        cast(product_id as varchar) as product_id,
        cast(seller_id as varchar) as seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        round(cast(price as double), 2) as price,
        round(cast(freight_value as double), 2) as freight_value

    from source

)

select * from renamed