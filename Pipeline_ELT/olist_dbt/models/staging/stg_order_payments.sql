with source as (

    select *
    from {{ source('olist_raw', 'raw_order_payments') }}

),

renamed as (

    select
        cast(order_id as varchar) as order_id,
        cast(payment_sequential as integer) as payment_sequential,
        cast(payment_type as varchar) as payment_type,
        cast(payment_installments as integer) as payment_installments,
        round(cast(payment_value as double), 2) as payment_value

    from source

)

select * from renamed