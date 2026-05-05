with products as (

    select *
    from {{ source('olist_raw', 'raw_products') }}

),

translation as (

    select *
    from {{ source('olist_raw', 'raw_category_translation') }}

),

joined as (

    select
        cast(p.product_id as varchar) as product_id,

        coalesce(
            lower(trim(cast(t.product_category_name_english as varchar))),
            'unknown'
        ) as product_category_name_english,

        coalesce(cast(p.product_weight_g as double), 0) as product_weight_g,
        coalesce(cast(p.product_length_cm as double), 0) as product_length_cm,
        coalesce(cast(p.product_height_cm as double), 0) as product_height_cm,
        coalesce(cast(p.product_width_cm as double), 0) as product_width_cm,
        coalesce(cast(p.product_photos_qty as integer), 0) as product_photos_qty

    from products p
    left join translation t
        on p.product_category_name = t.product_category_name

)

select * from joined