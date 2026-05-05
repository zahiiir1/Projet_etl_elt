select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,

    round(price + freight_value, 2) as total_amount,

    round(
        freight_value / nullif(price + freight_value, 0) * 100,
        2
    ) as freight_pct,

    case
        when price < 50 then 'cheap'
        when price < 200 then 'medium'
        else 'premium'
    end as price_range

from {{ ref('stg_order_items') }}